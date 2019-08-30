# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module SchemaStatements
        def execute(sql, name = nil)
          do_execute(sql, name)
        end

        def exec_insert(sql, name, _binds, _pk = nil, _sequence_name = nil)
          new_sql = sql.dup.sub(/ (DEFAULT )?VALUES/, " VALUES")
          do_execute(new_sql, name, format: nil)
          true
        end

        def exec_query(sql, name = nil, binds = [], prepare: false)
          result = do_execute(sql, name)
          ActiveRecord::Result.new(result['meta'].map { |m| m['name'] }, result['data'])
        end

        def exec_update(_sql, _name = nil, _binds = [])
          raise ActiveRecord::ActiveRecordError, 'Clickhouse update is not supported'
        end

        def exec_delete(_sql, _name = nil, _binds = [])
          raise ActiveRecord::ActiveRecordError, 'Clickhouse delete is not supported'
        end

        def tables(name = nil)
          result = do_system_execute('SHOW TABLES', name)
          return [] if result.nil?
          result['data'].flatten
        end

        def table_options(table)
          sql = do_system_execute("SHOW CREATE TABLE #{table}")['data'].try(:first).try(:first)
          { options: sql.gsub(/^(?:.*?)ENGINE = (.*?)$/, '\\1') }
        end

        # @todo copied from  ActiveRecord::ConnectionAdapters::SchemaStatements v5.2.2
        # Why version column type of String, but insert to Integer?
        def assume_migrated_upto_version(version, migrations_paths)
          migrations_paths = Array(migrations_paths)
          version = version.to_i
          sm_table = quote_table_name(ActiveRecord::SchemaMigration.table_name)

          migrated = ActiveRecord::SchemaMigration.all_versions.map(&:to_i)
          versions = migration_context.migration_files.map do |file|
            migration_context.parse_migration_filename(file).first.to_i
          end

          unless migrated.include?(version)
            do_execute( "INSERT INTO #{sm_table} (version) VALUES (#{quote(version.to_s)})", 'SchemaMigration', format: nil)
          end

          inserting = (versions - migrated).select { |v| v < version }
          if inserting.any?
            if (duplicate = inserting.detect { |v| inserting.count(v) > 1 })
              raise "Duplicate migration #{duplicate}. Please renumber your migrations to resolve the conflict."
            end
            if supports_multi_insert?
              do_system_execute insert_versions_sql(inserting.map(&:to_s))
            else
              inserting.each do |v|
                do_system_execute insert_versions_sql(v)
              end
            end
          end
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
            res = @connection.post("/?#{@config.to_param}", "#{sql} FORMAT JSONCompact")

            process_response(res)
          end
        end

        private

        def apply_format(sql, format)
          format ? "#{sql} FORMAT #{format}" : sql
        end

        def do_execute(sql, name = nil, format: 'JSONCompact')
          log(sql, "#{adapter_name} #{name}") do
            formatted_sql = apply_format(sql, format)
            res = @connection.post("/?#{@config.to_param}", formatted_sql)

            process_response(res)
          end
        end

        def process_response(res)
          case res.code.to_i
          when 200
            res.body.presence && JSON.parse(res.body)
          else
            raise ActiveRecord::ActiveRecordError,
              "Response code: #{res.code}:\n#{res.body}"
          end
        end

        def log_with_debug(sql, name = nil)
          return yield unless @debug
          log(sql, "#{name} (system)") { yield }
        end

        def schema_creation
          Clickhouse::SchemaCreation.new(self)
        end

        def create_table_definition(*args)
          Clickhouse::TableDefinition.new(*args)
        end

        def new_column_from_field(table_name, field)
          sql_type = field[1]
          type_metadata = fetch_type_metadata(sql_type)
          default = field[3]
          default_value = extract_value_from_default(default)
          default_function = extract_default_function(default_value, default)
          ClickhouseColumn.new(field[0], default_value, type_metadata, field[1].include?('Nullable'), default_function)
        end

        protected

        def table_structure(table_name)
          result = do_system_execute("DESCRIBE TABLE #{table_name}", table_name)
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
    end
  end
end
