# frozen_string_literal: true

require 'clickhouse-activerecord/version'

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module SchemaStatements
        DB_EXCEPTION_REGEXP = /\ACode:\s+\d+\.\s+DB::Exception:/.freeze

        def execute(sql, name = nil, settings: {})
          log(sql, "#{adapter_name} #{name}") do
            res = request(sql, settings)
            process_response(res, sql)
          end
        end

        def exec_insert(sql, name, _binds, _pk = nil, _sequence_name = nil, returning: nil)
          new_sql = sql.sub(/ (DEFAULT )?VALUES/, " VALUES")
          execute(new_sql, name)
          true
        end

        def internal_exec_query(sql, name = nil, binds = [], prepare: false, async: false, allow_retry: false)
          result = execute(sql, name)
          columns = result['meta'].map { |m| m['name'] }
          types = {}
          result['meta'].each_with_index do |m, i|
            # need use column name and index after commit in 7.2:
            # https://github.com/rails/rails/commit/24dbf7637b1d5cd6eb3d7100b8d0f6872c3fee3c
            types[m['name']] = types[i] = type_map.lookup(m['type'])
          end
          ActiveRecord::Result.new(columns, result['data'], types)
        rescue ActiveRecord::ActiveRecordError => e
          raise e
        rescue StandardError => e
          raise ActiveRecord::ActiveRecordError, "Response: #{e.message}"
        end

        def exec_insert_all(sql, name)
          execute(sql, name)
          true
        end

        # @link https://clickhouse.com/docs/en/sql-reference/statements/alter/update
        def exec_update(sql, name = nil, _binds = [])
          execute(sql, name)
          0
        end

        # @link https://clickhouse.com/docs/en/sql-reference/statements/delete
        def exec_delete(sql, name = nil, _binds = [])
          log(sql, "#{adapter_name} #{name}") do
            res = request(sql)
            begin
              data = JSON.parse(res.header['x-clickhouse-summary'])
              data['result_rows'].to_i
            rescue JSONError
              0
            end
          end
        end

        def tables(name = nil)
          result = do_system_execute("SHOW TABLES WHERE name NOT LIKE '.inner_id.%'", name)
          return [] if result.nil?
          result['data'].flatten
        end

        def views(name = nil)
          result = do_system_execute("SHOW TABLES WHERE engine = 'View'", name)
          return [] if result.nil?
          result['data'].flatten
        end

        def materialized_views(name = nil)
          result = do_system_execute("SHOW TABLES WHERE engine = 'MaterializedView'", name)
          return [] if result.nil?
          result['data'].flatten
        end

        def functions
          result = do_system_execute("SELECT name FROM system.functions WHERE origin = 'SQLUserDefined' ORDER BY name")
          return [] if result.nil?
          result['data'].flatten
        end

        def show_create_function(function)
          execute("SELECT create_query FROM system.functions WHERE origin = 'SQLUserDefined' AND name = '#{function}'").sub(/\ACREATE FUNCTION/, 'CREATE OR REPLACE FUNCTION')
        end

        def table_options(table)
          sql = show_create_table(table)
          { options: sql.gsub(/^(?:.*?)(?:ENGINE = (.*?))?( AS SELECT .*?)?$/, '\\1').presence, as: sql.match(/^CREATE (?:.*?) AS (SELECT .*?)$/).try(:[], 1) }.compact
        end

        # Not indexes on clickhouse
        def indexes(table_name, name = nil)
          []
        end

        def add_index_options(table_name, expression, **options)
          options.assert_valid_keys(:name, :type, :granularity, :first, :after, :if_not_exists, :if_exists)

          validate_index_length!(table_name, options[:name])

          IndexDefinition.new(table_name, options[:name], expression, options[:type], options[:granularity], first: options[:first], after: options[:after], if_not_exists: options[:if_not_exists], if_exists: options[:if_exists])
        end

        def data_sources
          tables
        end

        def do_system_execute(sql, name = nil)
          log_with_debug(sql, "#{adapter_name} #{name}") do
            res = request(sql)
            process_response(res, sql)
          end
        end

        if ::ActiveRecord::version >= Gem::Version.new('7.2')
          def schema_migration
            pool.schema_migration
          end

          def migration_context
            pool.migration_context
          end

          def internal_metadata
            pool.internal_metadata
          end
        end

        def assume_migrated_upto_version(version, migrations_paths = nil)
          version = version.to_i
          sm_table = quote_table_name(schema_migration.table_name)

          migrated = migration_context.get_all_versions
          versions = migration_context.migrations.map(&:version)

          unless migrated.include?(version)
            exec_insert "INSERT INTO #{sm_table} (version) VALUES (#{quote(version.to_s)})", nil, nil
          end

          inserting = (versions - migrated).select { |v| v < version }
          if inserting.any?
            if (duplicate = inserting.detect { |v| inserting.count(v) > 1 })
              raise "Duplicate migration #{duplicate}. Please renumber your migrations to resolve the conflict."
            end
            execute(insert_versions_sql(inserting), nil, settings: {max_partitions_per_insert_block: [100, inserting.size].max})
          end
        end

        # Fix insert_all method
        # https://github.com/PNixx/clickhouse-activerecord/issues/71#issuecomment-1923244983
        def with_yaml_fallback(value) # :nodoc:
          if value.is_a?(Array) || value.is_a?(Hash)
            value
          else
            super
          end
        end

        private

        # Make HTTP request to ClickHouse server
        # @param [String] sql
        # @param [Hash] settings
        # @return [Net::HTTPResponse]
        def request(sql, settings = {})
          formatted_sql = apply_format(sql)
          request_params = @connection_config || {}
          @lock.synchronize do
            @connection.post("/?#{request_params.merge(settings).to_param}", formatted_sql, {
              'User-Agent' => "Clickhouse ActiveRecord #{ClickhouseActiverecord::VERSION}",
              'Content-Type' => 'application/x-www-form-urlencoded',
            })
          end
        end

        def apply_format(sql)
          FormatManager.new(sql).apply
        end

        def process_response(res, sql = nil)
          case res.code.to_i
          when 200
            body = res.body

            if body.include?("DB::Exception") && body.match?(DB_EXCEPTION_REGEXP)
              raise ActiveRecord::ActiveRecordError, "Response code: #{res.code}:\n#{res.body}#{sql ? "\nQuery: #{sql}" : ''}"
            else
              format_body_response(res.body)
            end
          else
            case res.body
              when /DB::Exception:.*\(UNKNOWN_DATABASE\)/
                raise ActiveRecord::NoDatabaseError
              when /DB::Exception:.*\(DATABASE_ALREADY_EXISTS\)/
                raise ActiveRecord::DatabaseAlreadyExists
              else
                raise ActiveRecord::ActiveRecordError, "Response code: #{res.code}:\n#{res.body}"
            end
          end
        rescue JSON::ParserError
          res.body
        end

        def log_with_debug(sql, name = nil)
          return yield unless @debug
          log(sql, "#{name} (system)") { yield }
        end

        def schema_creation
          Clickhouse::SchemaCreation.new(self)
        end

        def create_table_definition(table_name, **options)
          Clickhouse::TableDefinition.new(self, table_name, **options)
        end

        def new_column_from_field(table_name, field, _definitions)
          sql_type = field[1]
          type_metadata = fetch_type_metadata(sql_type)
          default_value = extract_value_from_default(field[3], field[2])
          default_function = extract_default_function(field[3])
          default_value = lookup_cast_type(sql_type).cast(default_value)
          Clickhouse::Column.new(field[0], default_value, type_metadata, field[1].include?('Nullable'), default_function, codec: field[5].presence)
        end

        protected

        def table_structure(table_name)
          result = do_system_execute("DESCRIBE TABLE `#{table_name}`", table_name)
          data = result['data']

          return data unless data.empty?

          raise ActiveRecord::StatementInvalid, "Could not find table '#{table_name}'"
        end
        alias column_definitions table_structure

        private

        # Extracts the value from a PostgreSQL column default definition.
        def extract_value_from_default(default_expression, default_type)
          return nil if default_type != 'DEFAULT' || default_expression.blank?
          return nil if has_default_function?(default_expression)

          # Convert string
          return $1 if default_expression.match(/^'(.*?)'$/)

          default_expression
        end

        def extract_default_function(default) # :nodoc:
          default if has_default_function?(default)
        end

        def has_default_function?(default) # :nodoc:
          (%r{\w+\(.*\)} === default)
        end

        def format_body_response(body)
          return body if body.blank?

          format_from_json_compact_each_row_with_names_and_types(body)
        end

        def format_from_json_compact(body)
          parse_json_payload(body)
        end

        def format_from_json_compact_each_row_with_names_and_types(body)
          rows = body.split("\n").map { |row| parse_json_payload(row) }
          names, types, *data = rows

          meta = names.zip(types).map do |name, type|
            {
              'name' => name,
              'type' => type
            }
          end

          {
            'meta' => meta,
            'data' => data
          }
        end

        def parse_json_payload(payload)
          JSON.parse(payload, decimal_class: BigDecimal)
        end
      end
    end
  end
end
