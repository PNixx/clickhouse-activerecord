# frozen_string_literal: true

require 'arel/visitors/clickhouse'
require 'arel/nodes/final'
require 'arel/nodes/grouping_sets'
require 'arel/nodes/settings'
require 'arel/nodes/using'
require 'arel/nodes/limit_by'
require 'active_record/connection_adapters/clickhouse/oid/array'
require 'active_record/connection_adapters/clickhouse/oid/date'
require 'active_record/connection_adapters/clickhouse/oid/date_time'
require 'active_record/connection_adapters/clickhouse/oid/big_integer'
require 'active_record/connection_adapters/clickhouse/oid/map'
require 'active_record/connection_adapters/clickhouse/oid/uuid'
require 'active_record/connection_adapters/clickhouse/column'
require 'active_record/connection_adapters/clickhouse/quoting'
require 'active_record/connection_adapters/clickhouse/schema_creation'
require 'active_record/connection_adapters/clickhouse/schema_statements'
require 'active_record/connection_adapters/clickhouse/table_definition'
require 'net/http'
require 'openssl'

module ActiveRecord
  class Base
    class << self
      # Establishes a connection to the database that's used by all Active Record objects
      def clickhouse_connection(config)
        config = config.symbolize_keys

        unless config.key?(:database)
          raise ArgumentError, 'No database specified. Missing argument: database.'
        end

        ConnectionAdapters::ClickhouseAdapter.new(config)
      end
    end
  end

  module TypeCaster
    class Map
      def is_view
        if @klass.respond_to?(:is_view)
          @klass.is_view # rails 6.1
        else
          types.is_view # less than 6.1
        end
      end
    end
  end

  module ModelSchema
    module ClassMethods
      delegate :final, :final!,
               :group_by_grouping_sets, :group_by_grouping_sets!,
               :settings, :settings!,
               :window, :window!,
               :limit_by, :limit_by!,
               to: :all

      def is_view
        @is_view || false
      end
       # @param [Boolean] value
      def is_view=(value)
        @is_view = value
      end

      def _delete_record(constraints)
        raise ActiveRecord::ActiveRecordError.new('Deleting a row is not possible without a primary key') unless self.primary_key
        super
      end
    end
  end

  module ConnectionAdapters

    if ActiveRecord::version >= Gem::Version.new('7.2')
      register "clickhouse", "ActiveRecord::ConnectionAdapters::ClickhouseAdapter", "active_record/connection_adapters/clickhouse_adapter"
    end

    class ClickhouseAdapter < AbstractAdapter
      include Clickhouse::Quoting

      ADAPTER_NAME = 'Clickhouse'.freeze
      NATIVE_DATABASE_TYPES = {
        string: { name: 'String' },
        integer: { name: 'UInt32' },
        big_integer: { name: 'UInt64' },
        float: { name: 'Float32' },
        decimal: { name: 'Decimal' },
        datetime: { name: 'DateTime' },
        datetime64: { name: 'DateTime64' },
        date: { name: 'Date' },
        boolean: { name: 'Bool' },
        uuid: { name: 'UUID' },

        enum8: { name: 'Enum8' },
        enum16: { name: 'Enum16' },

        int8:  { name: 'Int8' },
        int16: { name: 'Int16' },
        int32: { name: 'Int32' },
        int64:  { name: 'Int64' },
        int128: { name: 'Int128' },
        int256: { name: 'Int256' },

        uint8: { name: 'UInt8' },
        uint16: { name: 'UInt16' },
        uint32: { name: 'UInt32' },
        uint64: { name: 'UInt64' },
        # uint128: { name: 'UInt128' }, not yet implemented in clickhouse
        uint256: { name: 'UInt256' },

        json: { name: 'JSON' },
      }.freeze

      include Clickhouse::SchemaStatements

      # Initializes and connects a Clickhouse adapter.
      def initialize(config_or_deprecated_connection, deprecated_logger = nil, deprecated_connection_options = nil, deprecated_config = nil)
        super
        if @config[:connection]
          connection = {
            connection: @config[:connection]
          }
        else
          port = @config[:port] || 8123
          connection = {
            host: @config[:host] || 'localhost',
            port: port,
            ssl: @config[:ssl].present? ? @config[:ssl] : port == 443,
            sslca: @config[:sslca],
            read_timeout: @config[:read_timeout],
            write_timeout: @config[:write_timeout],
            keep_alive_timeout: @config[:keep_alive_timeout]
          }
        end
        @connection_parameters = connection

        @connection_config = { user: @config[:username], password: @config[:password], database: @config[:database] }.compact
        @debug = @config[:debug] || false

        @prepared_statements = false

        connect
      end

      # Return ClickHouse server version
      def server_version
        @server_version ||= do_system_execute('SELECT version()')['data'][0][0]
      end

      # Savepoints are not supported, noop
      def create_savepoint(name)
      end

      def exec_rollback_to_savepoint(name)
      end

      def release_savepoint(name)
      end

      def migrations_paths
        @config[:migrations_paths] || 'db/migrate_clickhouse'
      end

      def arel_visitor # :nodoc:
        Arel::Visitors::Clickhouse.new(self)
      end

      def native_database_types #:nodoc:
        NATIVE_DATABASE_TYPES
      end

      def valid_type?(type)
        !native_database_types[type].nil?
      end

      def supports_indexes_in_create?
        true
      end

      class << self
        def extract_limit(sql_type) # :nodoc:
          case sql_type
            when /(Nullable)?\(?String\)?/
              super('String')
            when /(Nullable)?\(?U?Int8\)?/
              1
            when /(Nullable)?\(?U?Int16\)?/
              2
            when /(Nullable)?\(?U?Int32\)?/
              nil
            when /(Nullable)?\(?U?Int64\)?/
              8
            when /(Nullable)?\(?U?Int128\)?/
              16
            else
              super
          end
        end

        # `extract_scale` and `extract_precision` are the same as in the Rails abstract base class,
        # except this permits a space after the comma

        def extract_scale(sql_type)
          case sql_type
          when /\((\d+)\)/ then 0
          when /\((\d+)(,\s?(\d+))\)/ then $3.to_i
          end
        end

        def extract_precision(sql_type)
          $1.to_i if sql_type =~ /\((\d+)(,\s?\d+)?\)/
        end

        def initialize_type_map(m) # :nodoc:
          super
          register_class_with_limit m, %r(String), Type::String
          register_class_with_limit m, 'Date',  Clickhouse::OID::Date
          register_class_with_precision m, %r(datetime)i,  Clickhouse::OID::DateTime

          register_class_with_limit m, %r(Int8), Type::Integer
          register_class_with_limit m, %r(Int16), Type::Integer
          register_class_with_limit m, %r(Int32), Type::Integer
          register_class_with_limit m, %r(Int64), Type::Integer
          register_class_with_limit m, %r(Int128), Type::Integer
          register_class_with_limit m, %r(Int256), Type::Integer

          register_class_with_limit m, %r(UInt8), Type::UnsignedInteger
          register_class_with_limit m, %r(UInt16), Type::UnsignedInteger
          register_class_with_limit m, %r(UInt32), Type::UnsignedInteger
          register_class_with_limit m, %r(UInt64), Type::UnsignedInteger
          #register_class_with_limit m, %r(UInt128), Type::UnsignedInteger #not implemnted in clickhouse
          register_class_with_limit m, %r(UInt256), Type::UnsignedInteger

          m.register_type %r(bool)i, ActiveModel::Type::Boolean.new
          m.register_type %r{uuid}i, Clickhouse::OID::Uuid.new
          # register_class_with_limit m, %r(Array), Clickhouse::OID::Array
          m.register_type(%r(Array)) do |sql_type|
            Clickhouse::OID::Array.new(sql_type)
          end

          m.register_type(%r(Map)) do |sql_type|
            Clickhouse::OID::Map.new(sql_type)
          end

          m.register_type %r(JSON)i, ActiveRecord::Type::Json.new
        end
      end

      # In Rails 7 used constant TYPE_MAP, we need redefine method
      def type_map
        @type_map ||= Type::TypeMap.new.tap { |m| ClickhouseAdapter.initialize_type_map(m) }
      end

      def quote(value)
        case value
        when Array
          '[' + value.map { |v| quote(v) }.join(', ') + ']'
        when Hash
          '{' + value.map { |k, v| "#{quote(k)}: #{quote(v)}" }.join(', ') + '}'
        else
          super
        end
      end

      # Quoting time without microseconds
      def quoted_date(value)
        if value.acts_like?(:time)
          zone_conversion_method = ActiveRecord.default_timezone == :utc ? :getutc : :getlocal

          if value.respond_to?(zone_conversion_method)
            value = value.send(zone_conversion_method)
          end
        end

        value.to_fs(:db)
      end

      def column_name_for_operation(operation, node) # :nodoc:
        visitor.compile(node)
      end

      # Executes insert +sql+ statement in the context of this connection using
      # +binds+ as the bind substitutes. +name+ is logged along with
      # the executed +sql+ statement.

      # SCHEMA STATEMENTS ========================================

      def primary_keys(table_name)
        if server_version.to_f >= 23.4
          structure = do_system_execute("SHOW COLUMNS FROM `#{table_name}`")
          return structure['data'].select {|m| m[3]&.include?('PRI') }.pluck(0)
        end

        pk = table_structure(table_name).first
        return ['id'] if pk.present? && pk[0] == 'id'
        []
      end

      def create_schema_dumper(options) # :nodoc:
        ClickhouseActiverecord::SchemaDumper.create(self, options)
      end

      # @param [String] table
      # @option [Boolean] single_line
      # @return [String]
      def show_create_table(table, single_line: true)
        sql = do_system_execute("SHOW CREATE TABLE `#{table}`")['data'].try(:first).try(:first).gsub("#{@config[:database]}.", '')
        single_line ? sql.squish : sql
      end

      # Create a new ClickHouse database.
      def create_database(name)
        sql = apply_cluster "CREATE DATABASE #{quote_table_name(name)}"
        log_with_debug(sql, adapter_name) do
          res = @connection.post("/?#{@connection_config.except(:database).to_param}", sql)
          process_response(res, DEFAULT_RESPONSE_FORMAT)
        end
      end

      def create_view(table_name, request_settings: {}, **options)
        options.merge!(view: true)
        options = apply_replica(table_name, options)
        td = create_table_definition(apply_cluster(table_name), **options)
        yield td if block_given?

        if options[:force]
          drop_table(table_name, options.merge(if_exists: true))
        end

        do_execute(schema_creation.accept(td), format: nil, settings: request_settings)
      end

      def create_table(table_name, request_settings: {}, **options, &block)
        options = apply_replica(table_name, options)
        td = create_table_definition(apply_cluster(table_name), **options)
        block.call td if block_given?
        # support old migration version: in 5.0 options id: :integer, but 7.1 options empty
        # todo remove auto add id column in future
        if (!options.key?(:id) || options[:id].present? && options[:id] != false) && td[:id].blank? && options[:as].blank?
          td.column(:id, options[:id] || :integer, null: false)
        end

        if options[:force]
          drop_table(table_name, options.merge(if_exists: true))
        end

        do_execute(schema_creation.accept(td), format: nil, settings: request_settings)

        if options[:with_distributed]
          distributed_table_name = options.delete(:with_distributed)
          sharding_key = options.delete(:sharding_key) || 'rand()'
          raise 'Set a cluster' unless cluster

          distributed_options =
            "Distributed(#{cluster}, #{@connection_config[:database]}, #{table_name}, #{sharding_key})"
          create_table(distributed_table_name, **options.merge(options: distributed_options), &block)
        end
      end

      def create_function(name, body, **options)
        fd = "CREATE#{' OR REPLACE' if options[:force]} FUNCTION #{apply_cluster(quote_table_name(name))} AS #{body}"
        do_execute(fd, format: nil)
      end

      # Drops a ClickHouse database.
      def drop_database(name) #:nodoc:
        sql = apply_cluster "DROP DATABASE IF EXISTS #{quote_table_name(name)}"
        log_with_debug(sql, adapter_name) do
          res = @connection.post("/?#{@connection_config.except(:database).to_param}", sql)
          process_response(res, DEFAULT_RESPONSE_FORMAT)
        end
      end

      def drop_functions
        functions.each do |function|
          drop_function(function)
        end
      end

      def rename_table(table_name, new_name)
        do_execute apply_cluster "RENAME TABLE #{quote_table_name(table_name)} TO #{quote_table_name(new_name)}"
      end

      def drop_table(table_name, options = {}) # :nodoc:
        query = "DROP TABLE"
        query = "#{query} IF EXISTS " if options[:if_exists]
        query = "#{query} #{quote_table_name(table_name)}"
        query = apply_cluster(query)
        query = "#{query} SYNC" if options[:sync]

        do_execute(query)

        if options[:with_distributed]
          distributed_table_name = options.delete(:with_distributed)
          drop_table(distributed_table_name, **options)
        end
      end

      def drop_function(name, options = {})
        query = "DROP FUNCTION"
        query = "#{query} IF EXISTS " if options[:if_exists]
        query = "#{query} #{quote_table_name(name)}"
        query = apply_cluster(query)
        query = "#{query} SYNC" if options[:sync]

        do_execute(query, format: nil)
      end

      def add_column(table_name, column_name, type, **options)
        return if options[:if_not_exists] == true && column_exists?(table_name, column_name, type)

        at = create_alter_table table_name
        at.add_column(column_name, type, **options)
        execute(schema_creation.accept(at), nil, settings: {wait_end_of_query: 1, send_progress_in_http_headers: 1})
      end

      def remove_column(table_name, column_name, type = nil, **options)
        return if options[:if_exists] == true && !column_exists?(table_name, column_name)

        execute("ALTER TABLE #{quote_table_name(table_name)} #{remove_column_for_alter(table_name, column_name, type, **options)}", nil, settings: {wait_end_of_query: 1, send_progress_in_http_headers: 1})
      end

      def change_column(table_name, column_name, type, **options)
        result = do_execute("ALTER TABLE #{quote_table_name(table_name)} #{change_column_for_alter(table_name, column_name, type, **options)}", nil, settings: {wait_end_of_query: 1, send_progress_in_http_headers: 1})
        raise "Error parse json response: #{result}" if result.presence && !result.is_a?(Hash)
      end

      def change_column_null(table_name, column_name, null, default = nil)
        structure = table_structure(table_name).select{|v| v[0] == column_name.to_s}.first
        raise "Column #{column_name} not found in table #{table_name}" if structure.nil?
        change_column table_name, column_name, structure[1].gsub(/(Nullable\()?(.*?)\)?/, '\2'), {null: null, default: default}.compact
      end

      def change_column_default(table_name, column_name, default)
        change_column table_name, column_name, nil, {default: default}.compact
      end

      # Adds index description to tables metadata
      # @link https://clickhouse.com/docs/en/sql-reference/statements/alter/skipping-index
      def add_index(table_name, expression, **options)
        index = add_index_options(apply_cluster(table_name), expression, **options)
        execute schema_creation.accept(CreateIndexDefinition.new(index))
      end

      # Removes index description from tables metadata and deletes index files from disk
      def remove_index(table_name, name)
        query = apply_cluster("ALTER TABLE #{quote_table_name(table_name)}")
        execute "#{query} DROP INDEX #{quote_column_name(name)}"
      end

      # Rebuilds the secondary index name for the specified partition_name
      def rebuild_index(table_name, name, if_exists: false, partition: nil)
        query = [apply_cluster("ALTER TABLE #{quote_table_name(table_name)}")]
        query << 'MATERIALIZE INDEX'
        query << 'IF EXISTS' if if_exists
        query << quote_column_name(name)
        query << "IN PARTITION #{quote_column_name(partition)}" if partition
        execute query.join(' ')
      end

      # Deletes the secondary index files from disk without removing description
      def clear_index(table_name, name, if_exists: false, partition: nil)
        query = [apply_cluster("ALTER TABLE #{quote_table_name(table_name)}")]
        query << 'CLEAR INDEX'
        query << 'IF EXISTS' if if_exists
        query << quote_column_name(name)
        query << "IN PARTITION #{quote_column_name(partition)}" if partition
        execute query.join(' ')
      end

      def cluster
        @config[:cluster_name]
      end

      def replica
        @config[:replica_name]
      end

      def database
        @config[:database]
      end

      # Returns the shard name from the configuration.
      # This is used to identify the shard in replication paths when using both sharding and replication.
      # Required when you have multiple shards with replication to ensure unique paths for each shard's replication metadata.
      def shard
        @config[:shard_name]
      end

      def use_default_replicated_merge_tree_params?
        database_engine_atomic? && @config[:use_default_replicated_merge_tree_params]
      end

      def use_replica?
        (replica || use_default_replicated_merge_tree_params?) && cluster
      end

      # Returns the path for replication metadata.
      # When sharding is enabled (shard_name is set), the path includes the shard identifier
      # to ensure unique paths for each shard's replication metadata.
      # Format with sharding: /clickhouse/tables/{cluster}/{shard}/{database}.{table}
      # Format without sharding: /clickhouse/tables/{cluster}/{database}.{table}
      def replica_path(table)
        if shard
          "/clickhouse/tables/#{cluster}/#{shard}/#{@connection_config[:database]}.#{table}"
        else
          "/clickhouse/tables/#{cluster}/#{@connection_config[:database]}.#{table}"
        end
      end

      def database_engine_atomic?
        current_database_engine = "select engine from system.databases where name = '#{@connection_config[:database]}'"
        res = select_one(current_database_engine)
        res['engine'] == 'Atomic' if res
      end

      def apply_cluster(sql)
        if cluster
          normalized_cluster_name = cluster.start_with?('{') ? "'#{cluster}'" : cluster

          "#{sql} ON CLUSTER #{normalized_cluster_name}"
        else
          sql
        end
      end

      def supports_insert_on_duplicate_skip?
        true
      end

      def supports_insert_on_duplicate_update?
        true
      end

      def build_insert_sql(insert) # :nodoc:
        sql = +"INSERT #{insert.into} #{insert.values_list}"
        sql
      end

      protected

      def last_inserted_id(result)
        result
      end

      def change_column_for_alter(table_name, column_name, type, **options)
        td = create_table_definition(table_name)
        cd = td.new_column_definition(column_name, type, **options)
        schema_creation.accept(ChangeColumnDefinition.new(cd, column_name))
      end

      private

      def connect
        @connection = @connection_parameters[:connection] || Net::HTTP.start(@connection_parameters[:host], @connection_parameters[:port], use_ssl: @connection_parameters[:ssl], verify_mode: OpenSSL::SSL::VERIFY_NONE)

        @connection.ca_file = @connection_parameters[:ca_file] if @connection_parameters[:ca_file]
        @connection.read_timeout = @connection_parameters[:read_timeout] if @connection_parameters[:read_timeout]
        @connection.write_timeout = @connection_parameters[:write_timeout] if @connection_parameters[:write_timeout]

        # Use clickhouse default keep_alive_timeout value of 10, rather than Net::HTTP's default of 2
        @connection.keep_alive_timeout = @connection_parameters[:keep_alive_timeout] || 10

        @connection
      end

      def reconnect
        connect
      end

      def apply_replica(table, options)
        if use_replica? && options[:options]
          if options[:options].match(/^Replicated/)
            raise 'Do not try create Replicated table. It will be configured based on the *MergeTree engine.'
          end

          options[:options] = configure_replica(table, options[:options])
        end
        options
      end

      def configure_replica(table, options)
        match = options.match(/^(.*?MergeTree)(?:\(([^\)]*)\))?((?:.|\n)*)/)
        return options unless match

        if replica
          engine_params = ([replica_path(table), replica].map { |v| "'#{v}'" } + [match[2].presence]).compact.join(', ')
        end

        "Replicated#{match[1]}(#{engine_params})#{match[3]}"
      end
    end
  end
end
