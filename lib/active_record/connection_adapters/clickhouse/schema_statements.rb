# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module SchemaStatements
        def execute(sql, name = nil)
          do_execute(sql, name)
        end

        def exec_query(sql, name = nil, binds = [], prepare: false)
          result = do_execute(sql, name)
          ActiveRecord::Result.new(result['meta'].map { |m| m['name'] }, result['data'])
        end

        def table_structure(table_name)
          result = do_execute("DESCRIBE TABLE #{table_name}", table_name)
          data = result['data']

          raise(ActiveRecord::StatementInvalid, "Could not find table '#{table_name}'") if data.empty?

          data
        end
        alias column_definitions table_structure

        def tables(name = nil)
          result = do_execute('SHOW TABLES', name)
          result['data'].flatten
        end

        def data_sources
          tables
        end

        private

        def apply_format(sql, format)
          "#{sql} FORMAT #{format}"
        end

        def do_execute(sql, name = nil)
          formatted_sql = apply_format(sql, 'JSONCompact')

          log(formatted_sql, "#{adapter_name} #{name}") do
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

        def schema_creation
          Clickhouse::SchemaCreation.new(self)
        end

        def create_table_definition(*args)
          Clickhouse::TableDefinition.new(*args)
        end

        def new_column_from_field(table_name, field)
          sql_type = field[1]
          type_metadata = fetch_type_metadata(sql_type)
          ClickhouseColumn.new(field[0], field[3].present? ? field[3] : nil, type_metadata, field[1].include?('Nullable'), table_name, nil)
        end

        protected

        def table_structure(table_name)
          result = do_execute("DESCRIBE TABLE #{table_name}", table_name)
          data = result['data']

          return data unless data.empty?

          raise ActiveRecord::StatementInvalid,
            "Could not find table '#{table_name}'"
        end
        alias column_definitions table_structure
      end
    end
  end
end
