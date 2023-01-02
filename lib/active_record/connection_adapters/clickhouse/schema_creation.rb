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
          if options[:value]
            sql.gsub!(/\s+(.*)/, " \\1(#{options[:value]})")
          end
          if options[:fixed_string]
            sql.gsub!(/\s+(.*)/, " FixedString(#{options[:fixed_string]})")
          end
          if options[:null] || options[:null].nil?
            sql.gsub!(/\s+(.*)/, ' Nullable(\1)')
          end
          if options[:low_cardinality]
            sql.gsub!(/\s+(.*)/, ' LowCardinality(\1)')
          end
          if options[:array]
            sql.gsub!(/\s+(.*)/, ' Array(\1)')
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

        def add_as_clause!(create_sql, options)
          return unless options.as

          assign_database_to_subquery!(options.as) if options.view
          create_sql << " AS #{to_sql(options.as)}"
        end

        def assign_database_to_subquery!(subquery)
          # If you do not specify a database explicitly, ClickHouse will use the "default" database.
          return unless subquery

          match = subquery.match(/(?<=from)[^.\w]+(?<database>\w+(?=\.))?(?<table_name>[.\w]+)/i)
          return unless match
          return if match[:database]

          subquery[match.begin(:table_name)...match.end(:table_name)] =
            "#{current_database}.#{match[:table_name].sub('.', '')}"
        end

        def add_to_clause!(create_sql, options)
          # If you do not specify a database explicitly, ClickHouse will use the "default" database.
          return unless options.to

          match = options.to.match(/(?<database>.+(?=\.))?(?<table_name>.+)/i)
          return unless match
          return if match[:database]

          create_sql << "TO #{current_database}.#{match[:table_name].sub('.', '')}"
        end

        def visit_TableDefinition(o)
          create_sql = +"CREATE#{table_modifier_in_create(o)} #{o.view ? "VIEW" : "TABLE"} "
          create_sql << "IF NOT EXISTS " if o.if_not_exists
          create_sql << "#{quote_table_name(o.name)} "
          add_to_clause!(create_sql, o) if o.materialized

          statements = o.columns.map { |c| accept c }
          statements << accept(o.primary_keys) if o.primary_keys
          create_sql << "(#{statements.join(', ')})" if statements.present?
          # Attach options for only table or materialized view without TO section
          add_table_options!(create_sql, o) if !o.view || o.view && o.materialized && !o.to
          add_as_clause!(create_sql, o)
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

        def current_database
          ActiveRecord::Base.connection_db_config.database
        end
      end
    end
  end
end
