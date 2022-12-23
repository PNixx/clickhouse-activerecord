# frozen_string_literal: true

require 'active_record/connection_adapters/clickhouse/quoting'
require 'active_record/connection_adapters/clickhouse/schema_creation'
require 'active_record/connection_adapters/clickhouse/schema_definitions'
require 'active_record/connection_adapters/clickhouse/schema_dumper'
require 'active_record/connection_adapters/clickhouse/schema_statements'

require 'active_record/connection_adapters/clickhouse/oid/array'
require 'active_record/connection_adapters/clickhouse/oid/big_integer'
require 'active_record/connection_adapters/clickhouse/oid/date'
require 'active_record/connection_adapters/clickhouse/oid/date_time'

require 'arel/visitors/clickhouse'

require 'net/http'

module ActiveRecord
  module ConnectionAdapters
    class ClickhouseAdapter < AbstractAdapter

      ADAPTER_NAME = 'Clickhouse'
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

        int8: { name: 'Int8' },
        int16: { name: 'Int16' },
        int32: { name: 'Int32' },
        int64: { name: 'Int64' },
        int128: { name: 'Int128' },
        int256: { name: 'Int256' },

        uint8: { name: 'UInt8' },
        uint16: { name: 'UInt16' },
        uint32: { name: 'UInt32' },
        uint64: { name: 'UInt64' },
        uint128: { name: 'UInt128' },
        uint256: { name: 'UInt256' },
      }.freeze

      include Clickhouse::Quoting
      include Clickhouse::SchemaStatements

      init_type_map_definition =
        lambda { |m|
          super(m)

          register_class_with_limit m, %r(String), Type::String
          register_class_with_limit m, 'Date', Clickhouse::OID::Date
          register_class_with_limit m, 'DateTime', Clickhouse::OID::DateTime

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
          register_class_with_limit m, %r(UInt128), Type::UnsignedInteger
          register_class_with_limit m, %r(UInt256), Type::UnsignedInteger

          m.register_type(%r(Array)) do |sql_type|
            Clickhouse::OID::Array.new(sql_type)
          end
        }

      extract_limit_def =
        lambda { |sql_type|
          case sql_type
            when /(Nullable)?\(?String\)?/
              super('String')
            when /(?:Nullable)?\(?U?Int(\d+)\)?/
              $1.to_i / 8
            else
              super(sql_type)
          end
        }

      extract_scale_def =
        lambda { |sql_type|
          case sql_type
            when /\((\d+)\)/
              0
            when /\((\d+)(,\s?(\d+))\)/
              $3.to_i
          end
        }

      extract_precision_def = ->(sql_type) { $1.to_i if sql_type =~ /\((\d+)(,\s?\d+)?\)/ }

      if ActiveRecord.version < Gem::Version.new('7')
        define_method :initialize_type_map, &init_type_map_definition
        define_method :extract_limit, &extract_limit_def
        define_method :extract_scale, &extract_scale_def
        define_method :extract_precision, &extract_precision_def
      else
        define_singleton_method :initialize_type_map, &init_type_map_definition
        define_singleton_method :extract_limit, &extract_limit_def
        define_singleton_method :extract_scale, &extract_scale_def
        define_singleton_method :extract_precision, &extract_precision_def

        def initialize_type_map(m)
          self.class.initialize_type_map(m)
        end
      end

      # Initializes and connects a Clickhouse adapter.
      def initialize(logger, connection_parameters, config, full_config)
        super(nil, logger)
        @connection_parameters = connection_parameters
        @config = config
        @debug = full_config[:debug] || false
        @full_config = full_config

        @prepared_statements = false
        if ActiveRecord.version == Gem::Version.new('6.0.0')
          @prepared_statement_status = Concurrent::ThreadLocalVar.new(false)
        end

        connect
      end

      def migrations_paths
        @full_config[:migrations_paths] || 'db/migrate_clickhouse'
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

      # Quoting time without microseconds
      def quoted_date(value)
        if value.acts_like?(:time)
          default_timezone =
            if ActiveRecord.version >= Gem::Version.new('7')
              ActiveRecord.default_timezone
            else
              ActiveRecord::Base.default_timezone
            end
          zone_conversion_method = default_timezone == :utc ? :getutc : :getlocal

          if value.respond_to?(zone_conversion_method)
            value = value.send(zone_conversion_method)
          end
        end

        if ActiveRecord.version < Gem::Version.new('7')
          value.to_s(:db)
        else
          value.to_fs(:db)
        end
      end

      def column_name_for_operation(_operation, node) # :nodoc:
        if ActiveRecord.version >= Gem::Version.new('6')
          visitor.compile(node)
        else
          column_name_from_arel_node(node)
        end
      end

      # Executes insert +sql+ statement in the context of this connection using
      # +binds+ as the bind substitutes. +name+ is logged along with
      # the executed +sql+ statement.

      # SCHEMA STATEMENTS ========================================

      def primary_key(table_name) #:nodoc:
        pk = table_structure(table_name).first
        return 'id' if pk&.dig('name') == 'id'
        false
      end

      def create_schema_dumper(options) # :nodoc:
        Clickhouse::SchemaDumper.create(self, options)
      end

      # @param [String] table
      # @return [String]
      def show_create_table(table)
        do_system_execute("SHOW CREATE TABLE `#{table}`")['data'].try(:first).try(:first).gsub(/[\n\s]+/m, ' ')
      end

      # Create a new ClickHouse database.
      def create_database(name)
        sql = apply_cluster "CREATE DATABASE #{quote_table_name(name)}"
        log_with_debug(sql, adapter_name) do
          res = @connection.post("/?#{@config.except(:database).to_param}", sql)
          process_response(res)
        end
      end

      def create_view(table_name, **options)
        options.merge!(view: true)
        options = apply_replica(table_name, options)
        td = create_table_definition(apply_cluster(table_name), **options)
        yield td if block_given?

        if options[:force]
          drop_table(table_name, **options, if_exists: true)
        end

        execute schema_creation.accept td
      end

      def create_table(table_name, id: :primary_key, primary_key: nil, force: nil, **options, &block)
        options = apply_replica(table_name, options)

        result = super

        if options[:with_distributed]
          distributed_table_name = options.delete(:with_distributed)
          sharding_key = options.delete(:sharding_key) || 'rand()'
          raise 'Set a cluster' unless cluster

          distributed_options = "Distributed(#{cluster}, #{@config[:database]}, #{table_name}, #{sharding_key})"
          create_table(distributed_table_name,
                       id: id,
                       primary_key: primary_key,
                       force: force,
                       **options.merge(options: distributed_options),
                       &block)
        end

        result
      end

      def create_table_definition(table_name, **options)
        Clickhouse::TableDefinition.new(self, apply_cluster(table_name), **options)
      end

      # Drops a ClickHouse database.
      def drop_database(name) #:nodoc:
        sql = apply_cluster "DROP DATABASE IF EXISTS #{quote_table_name(name)}"
        log_with_debug(sql, adapter_name) do
          res = @connection.post("/?#{@config.except(:database).to_param}", sql)
          process_response(res)
        end
      end

      def rename_table(table_name, new_name)
        do_execute apply_cluster "RENAME TABLE #{quote_table_name(table_name)} TO #{quote_table_name(new_name)}"
      end

      def drop_table(table_name, **options) # :nodoc:
        do_execute apply_cluster "DROP TABLE#{' IF EXISTS' if options[:if_exists]} #{quote_table_name(table_name)}"

        if options[:with_distributed]
          distributed_table_name = options.delete(:with_distributed)
          drop_table(distributed_table_name, **options)
        end
      end

      def change_column(table_name, column_name, type, **options)
        result = do_execute "ALTER TABLE #{quote_table_name(table_name)} #{change_column_for_alter(table_name, column_name, type, **options)}"
        raise "Error parse json response: #{result}" if result.present? && !result.is_a?(Hash)
      end

      def change_column_null(table_name, column_name, null, default = nil)
        structure = table_structure(table_name).find { |v| v['name'] == column_name.to_s }
        raise "Column #{column_name} not found in table #{table_name}" if structure.nil?
        change_column_opts = { null: null, default: default }.compact
        change_column table_name, column_name, structure[1].gsub(/(Nullable\()?(.*?)\)?/, '\2'), **change_column_opts
      end

      def change_column_default(table_name, column_name, default)
        change_default_opts = { default: default }.compact
        change_column table_name, column_name, nil, **change_default_opts
      end

      def cluster
        @full_config[:cluster_name]
      end

      def replica
        @full_config[:replica_name]
      end

      def use_default_replicated_merge_tree_params?
        database_engine_atomic? && @full_config[:use_default_replicated_merge_tree_params]
      end

      def use_replica?
        (replica || use_default_replicated_merge_tree_params?) && cluster
      end

      def replica_path(table)
        "/clickhouse/tables/#{cluster}/#{@config[:database]}.#{table}"
      end

      def database_engine_atomic?
        @database_engine_atomic ||=
          begin
            current_database_engine = "select engine from system.databases where name = '#{@config[:database]}'"
            res = select_one(current_database_engine)
            res&.dig('engine') == 'Atomic'
          end
      end

      def apply_cluster(sql)
        return sql unless cluster

        normalized_cluster_name = cluster.start_with?('{') ? "'#{cluster}'" : cluster
        "#{sql} ON CLUSTER #{normalized_cluster_name}"
      end

      def supports_insert_on_duplicate_skip?
        true
      end

      def supports_insert_on_duplicate_update?
        true
      end

      def build_insert_sql(insert) # :nodoc:
        +"INSERT #{insert.into} #{insert.values_list}"
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

      def type_map
        @type_map ||= Type::TypeMap.new.tap(&method(:initialize_type_map))
      end

      def connect
        @connection               = @connection_parameters[:connection]
        @connection             ||= Net::HTTP.start(@connection_parameters[:host],
                                                      @connection_parameters[:port],
                                                      use_ssl:     @connection_parameters[:ssl],
                                                      verify_mode: OpenSSL::SSL::VERIFY_NONE)

        @connection.ca_file       = @connection_parameters[:ca_file] if @connection_parameters[:ca_file]
        @connection.read_timeout  = @connection_parameters[:read_timeout] if @connection_parameters[:read_timeout]
        @connection.write_timeout = @connection_parameters[:write_timeout] if @connection_parameters[:write_timeout]

        # Use clickhouse default keep_alive_timeout value of 10, rather than Net::HTTP's default of 2
        @connection.keep_alive_timeout = @connection_parameters[:keep_alive_timeout] || 10

        @connection
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
