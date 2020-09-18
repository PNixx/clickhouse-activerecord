# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class SchemaCreation < AbstractAdapter::SchemaCreation# :nodoc:

        def visit_AddColumnDefinition(o)
          +"ADD COLUMN #{accept(o.column)}"
        end

        def add_column_options!(sql, options)
          sql << " DEFAULT #{quote_default_expression(options[:default], options[:column])}" if options_include_default?(options)
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

        def visit_TableDefinition(o)
          create_sql = +"CREATE#{table_modifier_in_create(o)} #{o.view ? "VIEW" : "TABLE"} "
          create_sql << "IF NOT EXISTS " if o.if_not_exists
          create_sql << "#{quote_table_name(o.name)} "

          statements = o.columns.map { |c| accept c }
          statements << accept(o.primary_keys) if o.primary_keys

          create_sql << "(#{statements.join(', ')})" if statements.present?
          add_table_options!(create_sql, table_options(o))
          create_sql << " AS #{to_sql(o.as)}" if o.as
          create_sql
        end

        # Returns any SQL string to go between CREATE and TABLE. May be nil.
        def table_modifier_in_create(o)
          " TEMPORARY" if o.temporary
          " MATERIALIZED" if o.materialized
        end
      end
    end
  end
end
