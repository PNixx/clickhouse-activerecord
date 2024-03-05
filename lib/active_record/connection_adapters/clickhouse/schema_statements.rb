# frozen_string_literal: true

require 'clickhouse-activerecord/version'

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module SchemaStatements
        DEFAULT_RESPONSE_FORMAT = 'JSONCompactEachRowWithNamesAndTypes'.freeze

        def execute(sql, name = nil, settings: {})
          do_execute(sql, name, settings: settings)
        end

        def exec_insert(sql, name, _binds, _pk = nil, _sequence_name = nil, returning: nil)
          new_sql = sql.dup.sub(/ (DEFAULT )?VALUES/, " VALUES")
          do_execute(new_sql, name, format: nil)
          true
        end

        def internal_exec_query(sql, name = nil, binds = [], prepare: false, async: false)
          result = do_execute(sql, name)
          ActiveRecord::Result.new(result['meta'].map { |m| m['name'] }, result['data'], result['meta'].map { |m| [m['name'], type_map.lookup(m['type'])] }.to_h)
        rescue ActiveRecord::ActiveRecordError => e
          raise e
        rescue StandardError => e
          raise ActiveRecord::ActiveRecordError, "Response: #{e.message}"
        end

        def exec_insert_all(sql, name)
          do_execute(sql, name, format: nil)
          true
        end

        # @link https://clickhouse.com/docs/en/sql-reference/statements/alter/update
        def exec_update(_sql, _name = nil, _binds = [])
          do_execute(_sql, _name, format: nil)
          true
        end

        # @link https://clickhouse.com/docs/en/sql-reference/statements/delete
        def exec_delete(_sql, _name = nil, _binds = [])
          do_execute(_sql, _name, format: nil)
          true
        end

        def tables(name = nil)
          result = do_system_execute("SHOW TABLES WHERE name NOT LIKE '.inner_id.%'", name)
          return [] if result.nil?
          result['data'].flatten
        end

        def table_options(table)
          sql = show_create_table(table)
          { options: sql.gsub(/^(?:.*?)(?:ENGINE = (.*?))?( AS SELECT .*?)?$/, '\\1').presence, as: sql.match(/^CREATE (?:.*?) AS (SELECT .*?)$/).try(:[], 1) }.compact
        end

        # Not indexes on clickhouse
        def indexes(table_name, name = nil)
          []
        end

        def data_sources
          tables
        end

        def do_system_execute(sql, name = nil)
          log_with_debug(sql, "#{adapter_name} #{name}") do
            res = @connection.post("/?#{@connection_config.to_param}", "#{sql} FORMAT #{DEFAULT_RESPONSE_FORMAT}", 'User-Agent' => "Clickhouse ActiveRecord #{ClickhouseActiverecord::VERSION}")

            process_response(res, DEFAULT_RESPONSE_FORMAT)
          end
        end

        def do_execute(sql, name = nil, format: DEFAULT_RESPONSE_FORMAT, settings: {})
          log(sql, "#{adapter_name} #{name}") do
            formatted_sql = apply_format(sql, format)
            request_params = @connection_config || {}
            res = @connection.post("/?#{request_params.merge(settings).to_param}", formatted_sql, 'User-Agent' => "Clickhouse ActiveRecord #{ClickhouseActiverecord::VERSION}")

            process_response(res, format)
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
            do_execute(insert_versions_sql(inserting), nil, format: nil, settings: {max_partitions_per_insert_block: [100, inserting.size].max})
          end
        end

        # Fix insert_all method
        # https://github.com/PNixx/clickhouse-activerecord/issues/71#issuecomment-1923244983
        def with_yaml_fallback(value) # :nodoc:
          if value.is_a?(Array)
            value
          else
            super
          end
        end

        private

        def apply_format(sql, format)
          format ? "#{sql} FORMAT #{format}" : sql
        end

        def process_response(res, format)
          case res.code.to_i
          when 200
            if res.body.to_s.include?("DB::Exception")
              raise ActiveRecord::ActiveRecordError, "Response code: #{res.code}:\n#{res.body}"
            else
              format_body_response(res.body, format)
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
          default = field[3]
          default_value = extract_value_from_default(default)
          default_function = extract_default_function(default_value, default)
          ClickhouseColumn.new(field[0], default_value, type_metadata, field[1].include?('Nullable'), default_function)
        end

        protected

        def table_structure(table_name)
          result = do_system_execute("DESCRIBE TABLE `#{table_name}`", table_name)
          data = result['data']

          return data unless data.empty?

          raise ActiveRecord::StatementInvalid,
            "Could not find table '#{table_name}'"
        end
        alias column_definitions table_structure

        private

        # Extracts the value from a PostgreSQL column default definition.
        def extract_value_from_default(default)
          case default
            # Quoted types
          when /\Anow\(\)\z/m
            nil
            # Boolean types
          when "true".freeze, "false".freeze
            default
            # Object identifier types
          when "''"
            ''
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

        def format_body_response(body, format)
          return body if body.blank?

          case format
          when 'JSONCompact'
            format_from_json_compact(body)
          when 'JSONCompactEachRowWithNamesAndTypes'
            format_from_json_compact_each_row_with_names_and_types(body)
          else
            body
          end
        end

        def format_from_json_compact(body)
          JSON.parse(body)
        end

        def format_from_json_compact_each_row_with_names_and_types(body)
          rows = body.split("\n").map { |row| JSON.parse(row) }
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
      end
    end
  end
end
