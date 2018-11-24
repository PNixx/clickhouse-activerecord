
# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class SchemaCreation < AbstractAdapter::SchemaCreation# :nodoc:
        def visit_AddColumnDefinition(o)
          +"ADD COLUMN #{accept(o.column)}"
        end

        def add_column_options!(sql, options)
          if options[:null] == true
            sql.gsub(/\s+(.*)/, ' Nullable(\1)')
          else
            sql
          end
        end

        def add_table_options!(create_sql, options)
          if engine_sql = options[:options]
            create_sql << " ENGINE=#{engine_sql}"
          else
            create_sql << " ENGINE=Log()"
          end

          create_sql
        end
      end
    end
  end
end
