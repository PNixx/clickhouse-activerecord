# frozen_string_literal: true

require 'clickhouse-activerecord/version'

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module SchemaStatements
        def with_settings(**settings)
          @block_settings ||= {}
          prev_settings   = @block_settings
          @block_settings.merge! settings
          yield
        ensure
          @block_settings = prev_settings
        end

        def execute(sql, name = nil, format: 'JSONCompact', settings: {})
          log(sql, "#{adapter_name} #{name}") do
            formatted_sql = apply_format(sql, format)
            res = @connection.post("/?#{settings_params(settings)}", formatted_sql, 'User-Agent' => "Clickhouse ActiveRecord #{ClickhouseActiverecord::VERSION}")

            process_response(res)
          end
        end

        def exec_insert(sql, name, _binds, _pk = nil, _sequence_name = nil)
          new_sql = sql.dup.sub(/ (DEFAULT )?VALUES/, " VALUES")
          execute(new_sql, name, format: nil)
          true
        end

        def exec_query(sql, name = nil, _binds = [], _prepare: false)
          result = execute(sql, name)
          ActiveRecord::Result.new(result['meta'].map { |m| m['name'] }, result['data'])
        rescue ActiveRecord::ActiveRecordError => e
          raise e
        rescue StandardError => e
          raise ActiveRecord::ActiveRecordError, "Response: #{e.message}"
        end

        def exec_insert_all(sql, name)
          execute(sql, name, format: nil)
          true
        end

        def exec_update(_sql, _name = nil, _binds = [])
          raise ActiveRecord::ActiveRecordError, 'Clickhouse update is not supported'
        end

        def exec_delete(_sql, _name = nil, _binds = [])
          raise ActiveRecord::ActiveRecordError, 'Clickhouse delete is not supported'
        end

        def tables(name = nil)
          result = do_system_execute("SHOW TABLES WHERE name NOT LIKE '.inner_id.%'", name)
          return [] if result.nil?
          result['data'].flatten
        end

        def table_options(table)
          sql = show_create_table(table)
          { options: sql.gsub(/^.*?(?:ENGINE = (.*?))?( AS SELECT .*?)?$/, '\\1').presence, as: sql.match(/^CREATE.*? AS (SELECT .*?)$/).try(:[], 1) }.compact
        end

        # Not indexes on clickhouse
        def indexes(_table_name, _name = nil)
          []
        end

        def data_sources
          tables
        end

        def do_system_execute(sql, name = nil)
          log_with_debug(sql, "#{adapter_name} #{name}") do
            res = @connection.post("/?#{@config.to_param}", "#{sql} FORMAT JSONCompact", 'User-Agent' => "Clickhouse ActiveRecord #{ClickhouseActiverecord::VERSION}")

            process_response(res)
          end
        end

        def assume_migrated_upto_version(version, _migrations_paths = nil)
          version  = version.to_i
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
            settings = { max_partitions_per_insert_block: [100, inserting.size].max }
            execute insert_versions_sql(inserting), nil, settings: settings
          end
        end

        protected

        def table_structure(table_name)
          result = exec_query("DESCRIBE TABLE `#{table_name}`", table_name)
          raise ActiveRecord::StatementInvalid, "Could not find table '#{table_name}'" if result.empty?

          result
        end

        alias column_definitions table_structure

        private

        def apply_format(sql, format)
          return sql unless format

          "#{sql} FORMAT #{format}"
        end

        def process_response(res)
          raise ActiveRecord::ActiveRecordError, "Response code: #{res.code}:\n#{res.body}" unless res.code.to_i == 200

          JSON.parse(res.body) if res.body.present?
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

        def new_column_from_field(table_name, field)
          type_metadata = fetch_type_metadata(field['type'])

          raw_default      = field['default_expression']
          default_value    = extract_value_from_default(raw_default)
          default_function = extract_default_function(default_value, raw_default)

          if ActiveRecord.version >= Gem::Version.new('6')
            Column.new(field['name'], default_value, type_metadata, field['type'].include?('Nullable'), default_function, comment: field['comment'])
          else
            Column.new(field['name'], default_value, type_metadata, field['type'].include?('Nullable'), table_name, default_function, comment: field['comment'])
          end
        end

        # Extracts the value from a Clickhouse column raw_default definition.
        def extract_value_from_default(default)
          case default
          when "true", "false"
            default
          when /\A(-?\d+\.?\d*)\z/
            $1
          when /\A'(.*)'\z/
            unquote_string($1)
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
          %r{\w+\(.*\)}.match?(default) unless default_value
        end

        def settings_params(settings = {})
          request_params = @config || {}
          block_settings = @block_settings || {}
          request_params.merge(block_settings)
                        .merge(settings)
                        .to_param
        end
      end
    end
  end
end
