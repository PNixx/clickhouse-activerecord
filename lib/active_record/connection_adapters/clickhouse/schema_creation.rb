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
          if options[:map] == :array
            sql.gsub!(/\s+(.*)/, ' Map(String, Array(\1))')
          end
          if options[:map] == true
            sql.gsub!(/\s+(.*)/, ' Map(String, \1)')
          end
          if options[:codec]
            sql.gsub!(/\s+(.*)/, " \\1 CODEC(#{options[:codec]})")
          end
          sql.gsub!(/(\sString)\(\d+\)/, '\1')

          if ::ActiveRecord::version >= Gem::Version.new('8.1')
            sql << " DEFAULT #{quote_default_expression_for_column_definition(options[:default], options[:column])}" if options_include_default?(options)
          else
            sql << " DEFAULT #{quote_default_expression(options[:default], options[:column])}" if options_include_default?(options)
          end
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

          # Match FROM as a keyword (with word boundary), not as part of a column name
          # \b ensures we only match 'from' as a whole word
          match = subquery.match(/\bfrom\s+(?<database>\w+(?=\.))?(?<table_name>[.\w]+)/i)
          return unless match
          return if match[:database]

          subquery[match.begin(:table_name)...match.end(:table_name)] =
            "#{current_database}.#{match[:table_name].sub('.', '')}"
        end

        def add_materialized_to_clause!(create_sql, options)
          if !options.to
            create_sql << " ENGINE = Memory()"
          else
            target_table = options.to.split('.').last
            table_structure = @conn.execute("DESCRIBE TABLE #{target_table}")['data']
            column_definitions = table_structure.map do |field|
              "`#{field[0]}` #{field[1]}"
            end
            create_sql << "TO #{options.to} (#{column_definitions.join(', ')}) "
          end
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

          # Add column definitions for regular tables only
          if !o.view && o.columns.present?
            statements = o.columns.map { |c| accept c }
            statements << accept(o.primary_keys) if o.primary_keys

            if supports_indexes_in_create?
              indexes = o.indexes.map do |expression, options|
                accept(@conn.add_index_options(o.name, expression, **options))
              end
              statements.concat(indexes)
            end

            create_sql << "(#{statements.join(', ')})"
          end

          # Add TO clause for materialized views before AS clause
          add_materialized_to_clause!(create_sql, o) if o.materialized && o.view

          # Add AS clause for all views
          add_as_clause!(create_sql, o) if o.as

          # Add TO clause for regular views (non-materialized) after AS clause
          add_to_clause!(create_sql, o) if o.to && !o.materialized

          # Add table options for regular tables
          add_table_options!(create_sql, o) if !o.view

          create_sql
        end

        # Returns any SQL string to go between CREATE and TABLE. May be nil.
        def table_modifier_in_create(o)
          " TEMPORARY" if o.temporary
          " MATERIALIZED" if o.materialized
        end

        def visit_ChangeColumnDefinition(o)
          column = o.column
          column.sql_type = type_to_sql(column.type, **column.options)
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

        def visit_IndexDefinition(o, create = false)
          sql = create ? ["ALTER TABLE #{quote_table_name(o.table)} ADD"] : []
          sql << "INDEX"
          sql << "IF NOT EXISTS" if o.if_not_exists
          sql << "IF EXISTS" if o.if_exists
          sql << "#{quote_column_name(o.name)} (#{o.expression}) TYPE #{o.type}"
          sql << "GRANULARITY #{o.granularity}" if o.granularity
          sql << "FIRST #{quote_column_name(o.first)}" if o.first
          sql << "AFTER #{quote_column_name(o.after)}" if o.after

          sql.join(' ')
        end

        def visit_CreateIndexDefinition(o)
          visit_IndexDefinition(o.index, true)
        end

        def current_database
          ActiveRecord::Base.connection_db_config.database
        end
      end
    end
  end
end
