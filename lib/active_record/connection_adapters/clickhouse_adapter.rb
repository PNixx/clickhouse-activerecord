# frozen_string_literal: true

require 'active_record/connection_adapters/clickhouse/database_statements'
require 'active_record/connection_adapters/clickhouse/quoting'
require 'active_record/connection_adapters/clickhouse/schema_creation'
require 'active_record/connection_adapters/clickhouse/schema_definitions'
require 'active_record/connection_adapters/clickhouse/schema_dumper'
require 'active_record/connection_adapters/clickhouse/schema_statements'

require 'active_record/connection_adapters/clickhouse/oid/array'
require 'active_record/connection_adapters/clickhouse/oid/big_integer'
require 'active_record/connection_adapters/clickhouse/oid/date'
require 'active_record/connection_adapters/clickhouse/oid/date_time'
require 'active_record/connection_adapters/clickhouse/oid/tuple'
require 'active_record/connection_adapters/clickhouse/oid/uuid'

require 'arel/nodes/final'
require 'arel/nodes/settings'
require 'arel/visitors/clickhouse'

require 'net/http'
require 'openssl'

module ActiveRecord
  module ConnectionAdapters
    class ClickhouseAdapter < AbstractAdapter

      ADAPTER_NAME = 'Clickhouse'
      DEFAULT_FORMAT = 'JSONCompact'
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

        tuple: { name: 'Tuple' }
      }.freeze

      include Clickhouse::DatabaseStatements
      include Clickhouse::Quoting
      include Clickhouse::SchemaStatements

      init_type_map_definition =
        lambda { |m|
          super(m)

          register_class_with_limit m, %r(String), Type::String
          register_class_with_limit m, 'Date', Clickhouse::OID::Date
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
          register_class_with_limit m, %r(UInt128), Type::UnsignedInteger
          register_class_with_limit m, %r(UInt256), Type::UnsignedInteger

          m.register_type %r{uuid}i, Clickhouse::OID::Uuid.new

          m.register_type(%r(Array)) do |sql_type|
            Clickhouse::OID::Array.new(sql_type)
          end

          m.register_type(%r(Tuple)) do |sql_type|
            schema = Clickhouse::OID::Tuple.parse_schema(sql_type)
                                           .transform_values { |type| m.fetch(type) }
            Clickhouse::OID::Tuple.new(schema)
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
      def initialize(logger, connection_parameters, config)
        super(nil, logger)
        @connection_parameters = connection_parameters
        @config = config
        @connection_config = {
          user: @config[:username],
          password: @config[:password],
          database: @config[:database]
        }.compact
        @debug = @config[:debug] || false

        @prepared_statements = false

        connect
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

      def cluster
        @config[:cluster_name]
      end

      def replica
        @config[:replica_name]
      end

      def use_default_replicated_merge_tree_params?
        database_engine_atomic? && @config[:use_default_replicated_merge_tree_params]
      end

      def use_replica?
        (replica || use_default_replicated_merge_tree_params?) && cluster
      end

      def replica_path(table)
        "/clickhouse/tables/#{cluster}/#{@connection_config[:database]}.#{table}"
      end

      def database_engine_atomic?
        @database_engine_atomic ||=
          begin
            current_database_engine = "select engine from system.databases where name = '#{@connection_config[:database]}'"
            res = select_one(current_database_engine)
            res&.dig('engine') == 'Atomic'
          end
      end

      def supports_insert_on_duplicate_skip?
        true
      end

      def supports_insert_on_duplicate_update?
        true
      end

      def supports_update?
        database_version >= Gem::Version.new('23.3')
      end

      def supports_delete?
        database_version >= Gem::Version.new('23.3')
      end

      def build_insert_sql(insert) # :nodoc:
        +"INSERT #{insert.into} #{insert.values_list}"
      end

      def get_database_version
        Gem::Version.new(query_value('SELECT version()'))
      end

      private

      def type_map
        @type_map ||= Type::TypeMap.new.tap(&method(:initialize_type_map))
      end

      def connect
        @connection = @connection_parameters[:connection]
        @connection ||= Net::HTTP.start(@connection_parameters[:host],
                                        @connection_parameters[:port],
                                        use_ssl:     @connection_parameters[:ssl],
                                        verify_mode: OpenSSL::SSL::VERIFY_NONE)

        @connection.ca_file = @connection_parameters[:ca_file] if @connection_parameters[:ca_file]
        @connection.read_timeout = @connection_parameters[:read_timeout] if @connection_parameters[:read_timeout]
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

      def extract_new_default_value(default_or_changes)
        if default_or_changes.is_a?(Hash) && default_or_changes.key?(:from) && default_or_changes.key?(:to)
          default_or_changes[:to]
        else
          default_or_changes
        end
      end

      def strip_nullable(sql_type)
        return sql_type unless sql_type.start_with?('Nullable(')

        sql_type.match(/Nullable\((.*?)\)/)[1]
      end
    end
  end
end
