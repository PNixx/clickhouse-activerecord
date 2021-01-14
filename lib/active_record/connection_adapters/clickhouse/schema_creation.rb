# frozen_string_literal: true
begin
  require "active_record/connection_adapters/deduplicable"
rescue LoadError => e
  # Rails < 6.1 does not have this file in this location, ignore
end

require "active_record/connection_adapters/abstract/schema_creation"

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class SchemaCreation < ConnectionAdapters::SchemaCreation# :nodoc:

        def visit_AddColumnDefinition(o)
          sql = +"ADD COLUMN #{accept(o.column)}"
          sql << " AFTER " + quote_column_name(o.column.options[:after]) if o.column.options.key?(:after)
          sql
        end

        def add_column_options!(sql, options)
          if options[:null] || options[:null].nil?
            sql.gsub!(/\s+(.*)/, ' Nullable(\1)')
          end
          sql.gsub!(/(\sString)\(\d+\)/, '\1')
          sql << " DEFAULT #{quote_default_expression(options[:default], options[:column])}" if options_include_default?(options)
          sql
        end

        def add_table_options!(create_sql, options)
          opts = options[:options]
          if options.respond_to?(:options)
            # rails 6.1
            opts ||= options.options
          end
          
          if opts.present?
            create_sql << " ENGINE = #{opts}"
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
          add_table_options!(create_sql, o)
          create_sql << " AS #{to_sql(o.as)}" if o.as
          create_sql
        end

        # Returns any SQL string to go between CREATE and TABLE. May be nil.
        def table_modifier_in_create(o)
          " TEMPORARY" if o.temporary
          " MATERIALIZED" if o.materialized
        end

        def visit_ChangeColumnDefinition(o)
          column = o.column
          column.sql_type = type_to_sql(column.type, column.options)
          options = column_options(column)

          quoted_column_name = quote_column_name(o.name)
          type = column.sql_type
          type = "Nullable(#{type})" if options[:null]
          change_column_sql = +"MODIFY COLUMN #{quoted_column_name} #{type}"

          if options.key?(:default)
            quoted_default = quote_default_expression(options[:default], column)
            change_column_sql << " DEFAULT #{quoted_default}"
          end

          change_column_sql
        end

      end
    end
  end
end
