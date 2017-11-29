require 'clickhouse-activerecord/arel/visitors/to_sql'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/clickhouse/oid/date'
require 'active_record/connection_adapters/clickhouse/oid/date_time'
require 'active_record/connection_adapters/clickhouse/oid/big_integer'

module ActiveRecord
  class Base
    class << self
      # Establishes a connection to the database that's used by all Active Record objects
      def clickhouse_connection(config)
        config = config.symbolize_keys
        host = config[:host]
        port = config[:port] || 8123

        if config.key?(:database)
          database = config[:database]
        else
          raise ArgumentError, 'No database specified. Missing argument: database.'
        end

        ConnectionAdapters::ClickhouseAdapter.new(nil, logger, [host, port], { user: config[:username], password: config[:password], database: database }.compact)
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
    end

  end

  module ConnectionAdapters

    class ClickhouseColumn < Column
      def initialize(name, default, cast_type, sql_type = nil, null = true)
        super(name, default, cast_type, sql_type, null)
        @default_function = 'now'
      end

      private

      # Extracts the value from a PostgreSQL column default definition.
      def extract_value_from_default(default)
        case default
          # Quoted types
        when /\A[\(B]?'(.*)'.*::"?([\w. ]+)"?(?:\[\])?\z/m
          # The default 'now'::date is CURRENT_DATE
          if $1 == "now".freeze && $2 == "date".freeze
            nil
          else
            $1.gsub("''".freeze, "'".freeze)
          end
          # Boolean types
        when "true".freeze, "false".freeze
          default
          # Numeric types
        when /\A\(?(-?\d+(\.\d*)?)\)?(::bigint)?\z/
          $1
          # Object identifier types
        when /\A-?\d+\z/
          $1
        else
          # Anything else is blank, some user type, or some function
          # and we can't know the value of that, so return nil.
          nil
        end
      end

      def extract_default_function(default_value, default) # :nodoc:
        default if has_default_function?(default_value, default)
      end

      def has_default_function?(default_value, default) # :nodoc:
        !default_value && (%r{\w+\(.*\)} === default)
      end

    end

    class ClickhouseAdapter < AbstractAdapter
      ADAPTER_NAME = 'Clickhouse'.freeze

      NATIVE_DATABASE_TYPES = {
        string: { name: 'String', limit: 255 },
        integer: { name: 'UInt32' },
        big_integer: { name: 'UInt64', limit: 8 },
        float: { name: 'Float32' },
        datetime: { name: 'DateTime' },
        date: { name: 'Date' },
        boolean: { name: 'UInt8' }
      }.freeze

      # Initializes and connects a Clickhouse adapter.
      def initialize(connection, logger, connection_parameters, config)
        super(connection, logger)
        @connection_parameters = connection_parameters
        @config = config

        @visitor = ClickhouseActiverecord::Arel::Visitors::ToSql.new self

        connect
      end

      def native_database_types #:nodoc:
        NATIVE_DATABASE_TYPES
      end

      def valid_type?(type)
        !native_database_types[type].nil?
      end

      def extract_limit(sql_type) # :nodoc:
        case sql_type
          when 'Nullable(String)'
            255
          else
            super
        end
      end

      def initialize_type_map(m) # :nodoc:
        super
        register_class_with_limit m, 'String', Type::String
        register_class_with_limit m, 'Nullable(String)', Type::String
        register_class_with_limit m, 'Uint8', Type::UnsignedInteger
        register_class_with_limit m, 'Date',  Clickhouse::OID::Date
        register_class_with_limit m, 'DateTime',  Clickhouse::OID::DateTime
        m.alias_type 'UInt16', 'uint8'
        m.alias_type 'UInt32', 'uint8'
        m.register_type 'UInt64', Clickhouse::OID::BigInteger.new
        m.alias_type 'Int8', 'int8'
        m.alias_type 'Int16', 'int8'
        m.alias_type 'Int32', 'int8'
        m.alias_type 'Int64', 'UInt64'
      end

      # Queries the database and returns the results in an Array-like object
      def query(sql, _name = nil)
        res = @connection.get("/?#{@config.merge(query: "#{sql} FORMAT JSONCompact").to_param}")
        raise ActiveRecord::ActiveRecordError, "Response code: #{res.code}:\n#{res.body}" unless res.code.to_i == 200
        JSON.parse res.body
      end

      def execute(sql, name = nil)
        log(sql, "#{adapter_name} #{name}") do
          query sql, name
        end
      end

      def exec_query(sql, name = nil, _binds = [])
        result = execute(sql, name)
        ActiveRecord::Result.new(result['meta'].map { |m| m['name'] }, result['data'])
      end

      # Executes insert +sql+ statement in the context of this connection using
      # +binds+ as the bind substitutes. +name+ is logged along with
      # the executed +sql+ statement.
      def exec_insert(sql, name, _binds, _pk = nil, _sequence_name = nil)
        log(sql, "#{adapter_name} #{name}") do
          res = @connection.post("/?#{@config.to_param}", sql)
          raise ActiveRecord::ActiveRecordError, "Response code: #{res.code}:\n#{res.body}" unless res.code.to_i == 200
          true
        end
      end

      def update(_arel, _name = nil, _binds = [])
        raise ActiveRecord::ActiveRecordError, 'Clickhouse update is not supported'
      end

      def delete(_arel, _name = nil, _binds = [])
        raise ActiveRecord::ActiveRecordError, 'Clickhouse delete is not supported'
      end

      # SCHEMA STATEMENTS ========================================

      def tables(name = nil)
        query('SHOW TABLES', name)['data'].flatten
      end

      def columns(table_name, _name = nil) #:nodoc:
        table_structure(table_name).map do |field|
          ClickhouseColumn.new(field[0], field[3].present? ? field[3] : nil, lookup_cast_type(field[1]), field[1], field[1].include?('Nullable'))
        end
      end

      def indexes(_table_name, _name = nil) #:nodoc:
        Rails.logger.warn 'Clickhouse indexes is not supported'
        []
      end

      def primary_key(table_name) #:nodoc:
        pk = table_structure(table_name).first
        return 'id' if pk.present? && pk[0] == 'id'
        false
      end

      protected

      def table_structure(table_name)
        data = query("DESCRIBE TABLE #{table_name}", table_name)['data']
        raise(ActiveRecord::StatementInvalid, "Could not find table '#{table_name}'") if data.empty?
        data
      end

      def last_inserted_id(result)
        result
      end

      private

      def connect
        @connection = Net::HTTP.start(@connection_parameters[0], @connection_parameters[1])
      end
    end
  end
end
