# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class SchemaCreation < AbstractAdapter::SchemaCreation# :nodoc:

        def visit_AddColumnDefinition(o)
          +"ADD COLUMN #{accept(o.column)}"
        end

        def add_column_options!(sql, options)
          if options[:null] || options[:null].nil?
            sql.gsub!(/\s+(.*)/, ' Nullable(\1)')
          end
          sql.gsub!(/(\sString)\(\d+\)/, '\1')
          sql
        end

        def add_table_options!(create_sql, options)
          if options[:options].present?
            create_sql << " ENGINE = #{options[:options]}"
          else
            create_sql << " ENGINE = Log()"
          end

          create_sql
        end
      end
    end
  end
end
