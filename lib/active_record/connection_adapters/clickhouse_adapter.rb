# frozen_string_literal: true

require 'clickhouse-activerecord/arel/visitors/to_sql'
require 'clickhouse-activerecord/arel/table'
require 'clickhouse-activerecord/arel/nodes/using'
require 'clickhouse-activerecord/arel/nodes/functions'
require 'clickhouse-activerecord/arel/nodes/to'
require 'clickhouse-activerecord/arel/nodes/limit_by'
require 'clickhouse-activerecord/arel/extensions/functions'
require 'clickhouse-activerecord/arel/extensions/node_expressions'
require 'clickhouse-activerecord/arel/select_statement'
require 'clickhouse-activerecord/arel/select_manager'
require 'active_record/connection_adapters/clickhouse/oid/date'
require 'active_record/connection_adapters/clickhouse/oid/date_time'
require 'active_record/connection_adapters/clickhouse/oid/big_integer'
require 'active_record/connection_adapters/clickhouse/schema_definitions'
require 'active_record/connection_adapters/clickhouse/schema_creation'
require 'active_record/connection_adapters/clickhouse/schema_statements'
require 'net/http'

module ActiveRecord
  class Base
    class << self

      @@clickhouse_connection_adapter_pool = {}

      # Establishes a connection to the database that's used by all Active Record objects
      def clickhouse_connection(config)
        config = config.symbolize_keys

        @@clickhouse_connection_adapter_pool[config] ||= begin
                                      raise ArgumentError, 'No database specified. Missing argument: database.' unless config.key?(:database)

                                      connection_params = if config[:urls]
                                                            {
                                                              urls: config[:urls]
                                                            }
                                                          else
                                                            {
                                                              host: config[:host] || 'localhost',
                                                              port: config[:port] || 8123,
                                                              ssl: config[:ssl].present? ? config[:ssl] : config[:port] == 443
                                                            }
                                                          end

                                      auth_params = { user: config[:username], password: config[:password], database: config[:database] }
                                      ConnectionAdapters::ClickhouseAdapter.new(logger,
                                                                                connection_params.compact,
                                                                                auth_params.compact,
                                                                                config)
                                    end


      end
    end
  end

  class Relation

    # Replace for only ClickhouseAdapter
    def reverse_order!
      orders = order_values.uniq
      orders.reject!(&:blank?)
      if self.connection.is_a?(ConnectionAdapters::ClickhouseAdapter) && orders.empty? && !primary_key
        self.order_values = %w(date created_at).select { |c| column_names.include?(c) }.map { |c| arel_attribute(c).desc }
      else
        self.order_values = reverse_sql_order(orders)
      end
      self
    end
  end

  module TypeCaster
    class Map
      def is_view
        types.is_view
      end
    end
  end

  module ModelSchema
    module ClassMethods
      def is_view
        @is_view || false
      end

      # @param [Boolean] value
      def is_view=(value)
        @is_view = value
      end

      def arel_table # :nodoc:
        @arel_table ||= ClickhouseActiverecord::Arel::Table.new(table_name, type_caster: type_caster)
      end

    end
  end

  module ConnectionAdapters
    class ClickhouseColumn < Column

    end

    class Cluster

      attr_reader :urls, :connections, :adapter

      def initialize params, adapter
        @urls =  params[:urls] || [build_url(params)]
        @connections = []
        @adapter = adapter
      end

      def post url, data

        connection, index = select_connection

        if !connection
          raise "No connection found for #{urls.join(',')}"
        end

        begin
          connection.post url, data
        rescue
          connections[index]  = nil
          close_connection connection
          raise
        end

      end

      private

      def build_url(params)
        (params[:use_ssl] ?
          URI::HTTPS.build(host: params[:host], port: params[:port]) :
          URI::HTTP.build(host: params[:host], port: params[:port])).to_s
      end

      def close_connection connection
        begin
          connection.finish
        rescue
        end
      end

      def select_connection
        index = if adapter.session_id
                   Digest::MD5.hexdigest(adapter.session_id).to_i(16) % urls.size
                 else
                   Random.rand urls.size
                end
        [(connections[index] ||= connect(index)),index]
      end

      def connect index

        begin
          return try_connect(index)
        rescue
          raise if urls.size==1
        end

        (0...urls.count).each do |i|
          next if i==index
          begin
            return try_connect(i)
          rescue
            raise if i == urls.count - 1 || index==urls.count - 1
          end
        end

      end

      def try_connect index
        url = URI urls[index]
        return Net::HTTP.start(url.host, url.port,
                               use_ssl: (url.scheme == 'https' || url.port == 443),
                               verify_mode: OpenSSL::SSL::VERIFY_NONE)
      end


    end

    class ClickhouseAdapter < AbstractAdapter
      ADAPTER_NAME = 'Clickhouse'.freeze

      NATIVE_DATABASE_TYPES = {
        string: { name: 'String' },
        integer: { name: 'UInt32' },
        big_integer: { name: 'UInt64' },
        float: { name: 'Float32' },
        decimal: { name: 'Decimal' },
        datetime: { name: 'DateTime' },
        date: { name: 'Date' },
        boolean: { name: 'UInt8' }
      }.freeze

      include Clickhouse::SchemaStatements

      attr_reader :stat,  :session_id

      # Initializes and connects a Clickhouse adapter.
      def initialize(logger, connection_parameters, config, full_config)
        super(nil, logger)

        @config = config
        @debug = !!full_config[:debug]
        @full_config = full_config

        @prepared_statements = false
        if ActiveRecord::version == Gem::Version.new('6.0.0')
          @prepared_statement_status = Concurrent::ThreadLocalVar.new(false)
        end

        @stat = nil
        @saved_stat = []

        @cluster = Cluster.new connection_parameters, self
      end

      def with_statistics stat

        @saved_stat.push @stat
        @stat = stat

        yield

      ensure
        @stat = @saved_stat.pop
        @session_id = nil
      end

      # Support SchemaMigration from v5.2.2 to v6+
      def schema_migration # :nodoc:
        if ActiveRecord::version >= Gem::Version.new('6')
          super
        else
          ActiveRecord::SchemaMigration
        end
      end

      def migrations_paths
        @full_config[:migrations_paths] || 'db/migrate_clickhouse'
      end

      def arel_visitor # :nodoc:
        ClickhouseActiverecord::Arel::Visitors::ToSql.new(self)
      end

      def native_database_types #:nodoc:
        NATIVE_DATABASE_TYPES
      end

      def valid_type?(type)
        !native_database_types[type].nil?
      end

      def extract_limit(sql_type)
        # :nodoc:
        case sql_type
        when /(Nullable)?\(?String\)?/
          super('String')
        when /(Nullable)?\(?U?Int8\)?/
          super('int2')
        when /(Nullable)?\(?U?Int(16|32)\)?/
          super('int4')
        when /(Nullable)?\(?U?Int(64)\)?/
          8
        else
          super
        end
      end

      def initialize_type_map(m)
        # :nodoc:
        super
        register_class_with_limit m, %r(String), Type::String
        register_class_with_limit m, %r(Enum), Type::String
        register_class_with_limit m, 'Date', Clickhouse::OID::Date
        register_class_with_limit m, 'DateTime', Clickhouse::OID::DateTime

        register_class_with_limit m, %r(Int8), Type::Integer
        register_class_with_limit m, %r(Int16), Type::Integer
        register_class_with_limit m, %r(Int32), Type::Integer
        register_class_with_limit m, %r(Int64), Type::Integer

        register_class_with_limit m, %r(UInt8), Type::UnsignedInteger
        register_class_with_limit m, %r(UInt16), Type::UnsignedInteger
        register_class_with_limit m, %r(UInt32), Type::UnsignedInteger
        register_class_with_limit m, %r(UInt64), Type::UnsignedInteger

      end

      # Quoting time without microseconds
      def quoted_date(value)
        if value.acts_like?(:time)
          zone_conversion_method = ActiveRecord::Base.default_timezone == :utc ? :getutc : :getlocal

          if value.respond_to?(zone_conversion_method)
            value = value.send(zone_conversion_method)
          end
        end

        value.to_s(:db)
      end

      def column_name_for_operation(operation, node)
        # :nodoc:
        if ActiveRecord::version >= Gem::Version.new('6')
          visitor.compile(node)
        else
          column_name_from_arel_node(node)
        end
      end

      # Executes insert +sql+ statement in the context of this connection using
      # +binds+ as the bind substitutes. +name+ is logged along with
      # the executed +sql+ statement.

      # SCHEMA STATEMENTS ========================================

      def primary_key(table_name)
        #:nodoc:
        pk = table_structure(table_name).first
        return 'id' if pk.present? && pk[0] == 'id'
        false
      end

      def create_schema_dumper(options)
        # :nodoc:
        ClickhouseActiverecord::SchemaDumper.create(self, options)
      end

      # Create a new ClickHouse database.
      def create_database(name)
        sql = "CREATE DATABASE #{quote_table_name(name)}"
        log_with_debug(sql, adapter_name) do
          res = @cluster.post("/?#{@config.except(:database).to_param}", "CREATE DATABASE #{quote_table_name(name)}")
          process_response(res)
        end
      end

      # Drops a ClickHouse database.
      def drop_database(name)
        #:nodoc:
        sql = "DROP DATABASE IF EXISTS #{quote_table_name(name)}"
        log_with_debug(sql, adapter_name) do
          res = @cluster.post("/?#{@config.except(:database).to_param}", sql)
          process_response(res)
        end
      end

      def drop_table(table_name, options = {})
        # :nodoc:
        do_execute "DROP TABLE#{' IF EXISTS' if options[:if_exists]} #{quote_table_name(table_name)}"
      end

      def database_name
        @config[:database] || 'default'
      end

      def full_table_name table_name
        @config[:database] ? "`#{database_name}`.`#{table_name}`" : table_name
      end

      protected

      def last_inserted_id(result)
        result
      end


    end
  end
end
