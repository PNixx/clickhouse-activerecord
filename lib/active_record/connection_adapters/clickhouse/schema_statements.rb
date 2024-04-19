# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module SchemaStatements
        # Create a new ClickHouse database.
        def create_database(name)
          sql = apply_cluster "CREATE DATABASE #{quote_table_name(name)}"
          do_system_execute sql, adapter_name, except_params: [:database]
        end

        # Drops a ClickHouse database.
        def drop_database(name) #:nodoc:
          sql = apply_cluster "DROP DATABASE IF EXISTS #{quote_table_name(name)}"
          do_system_execute sql, adapter_name, except_params: [:database]
        end

        def create_table(table_name, id: :primary_key, primary_key: nil, force: nil, **options, &block)
          options = apply_replica(table_name, options)

          result = super

          if options[:with_distributed]
            distributed_table_name = options.delete(:with_distributed)
            sharding_key = options.delete(:sharding_key) || 'rand()'
            raise 'Set a cluster' unless cluster

            distributed_options = "Distributed(#{cluster}, #{@connection_config[:database]}, #{table_name}, #{sharding_key})"
            create_table(distributed_table_name,
                         id: id,
                         primary_key: primary_key,
                         force: force,
                         **options.merge(options: distributed_options),
                         &block)
          end

          result
        end

        def rename_table(table_name, new_name)
          execute apply_cluster "RENAME TABLE #{quote_table_name(table_name)} TO #{quote_table_name(new_name)}"
        end

        def drop_table(table_name, **options) # :nodoc:
          query = "DROP TABLE"
          query = "#{query} IF EXISTS " if options[:if_exists]
          query = "#{query} #{quote_table_name(table_name)}"
          query = apply_cluster(query)
          query = "#{query} SYNC" if options[:sync]

          execute(query)

          if options[:with_distributed]
            distributed_table_name = options.delete(:with_distributed)
            drop_table(distributed_table_name, **options)
          end
        end

        def tables(name = nil)
          result = do_system_execute("SHOW TABLES WHERE name NOT LIKE '.inner_id.%'", name)
          return [] if result.nil?
          result['data'].flatten
        end

        def table_options(table)
          sql = show_create_table(table)
          {
            options: sql.gsub(/^.*?(?:ENGINE = (.*?))?( AS SELECT .*?)?$/, '\\1').presence,
            as: sql.match(/^CREATE.*? AS (SELECT .*?)$/).try(:[], 1)
          }.compact
        end

        def primary_key(table_name) #:nodoc:
          pk = table_structure(table_name).first
          return 'id' if pk&.dig('name') == 'id'
          false
        end

        # @param [String] table
        # @return [String]
        def show_create_table(table)
          do_system_execute("SHOW CREATE TABLE `#{table}`")['data'].try(:first).try(:first).gsub(/[\n\s]+/m, ' ')
        end

        def create_view(table_name, **options)
          options.merge!(view: true)
          options = apply_replica(table_name, options)
          td = create_table_definition(apply_cluster(table_name), **options)
          yield td if block_given?

          drop_table(table_name, **options, if_exists: true) if options[:force]

          execute schema_creation.accept td
        end

        def add_column(table_name, column_name, type, **options)
          with_settings(wait_end_of_query: 1, send_progress_in_http_headers: 1) { super }
        end

        def remove_column(table_name, column_name, type = nil, **options)
          with_settings(wait_end_of_query: 1, send_progress_in_http_headers: 1) { super }
        end

        def change_column(table_name, column_name, type, **options)
          result = execute "ALTER TABLE #{quote_table_name(table_name)} #{change_column_for_alter(table_name, column_name, type, **options)}"
          raise "Error parse json response: #{result}" if result.present? && !result.is_a?(Hash)
        end

        def change_column_null(table_name, column_name, null, default = nil)
          raise(ActiveRecordError, <<~MSG.squish) if !null && default
            Cannot set temporary default when changing column nullability;
            ClickHouse does not support UPDATE statements. Please manually
            update NULL values before making column non-nullable.
          MSG

          column = column_for(table_name, column_name)
          change_column table_name, column_name, strip_nullable(column.sql_type), null: null
        end

        def change_column_default(table_name, column_name, default_or_changes)
          change_column table_name, column_name, nil, default: extract_new_default_value(default_or_changes)
        end

        def create_function(name, body)
          execute "CREATE FUNCTION #{apply_cluster(quote_table_name(name))} AS #{body}"
        end

        def drop_functions
          functions.each do |function|
            drop_function(function)
          end
        end

        def drop_function(name, options = {})
          query = +'DROP FUNCTION'
          query << ' IF EXISTS' if options[:if_exists]
          query << " #{quote_table_name(name)}"
          query = apply_cluster(query)
          query << ' SYNC' if options[:sync]

          execute(query)
        end

        def functions
          result = do_system_execute("SELECT name FROM system.functions WHERE origin = 'SQLUserDefined'")
          return [] if result.nil?
          result['data'].flatten
        end

        def show_create_function(function)
          execute("SELECT create_query FROM system.functions WHERE origin = 'SQLUserDefined' AND name = '#{function}'", format: nil)
        end

        # Not indexes on clickhouse
        def indexes(_table_name, _name = nil)
          []
        end

        def data_sources
          tables
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

        def create_schema_dumper(options) # :nodoc:
          Clickhouse::SchemaDumper.create(self, options)
        end

        def valid_column_definition_options # :nodoc:
          super + %i[after array fixed_string low_cardinality value]
        end

        protected

        def table_structure(table_name)
          result = exec_query("DESCRIBE TABLE `#{table_name}`", table_name)
          raise ActiveRecord::StatementInvalid, "Could not find table '#{table_name}'" if result.empty?

          result
        end

        alias column_definitions table_structure

        def change_column_for_alter(table_name, column_name, type, **options)
          td = create_table_definition(table_name)
          cd = td.new_column_definition(column_name, type, **options)
          schema_creation.accept(ChangeColumnDefinition.new(cd, column_name))
        end

        private

        def schema_creation
          Clickhouse::SchemaCreation.new(self)
        end

        def create_table_definition(table_name, **options)
          Clickhouse::TableDefinition.new(self, apply_cluster(table_name), **options)
        end

        def new_column_from_field(table_name, field, _definitions = nil)
          type_metadata = fetch_type_metadata(field['type'])

          raw_default      = field['default_expression']
          default_value    = extract_value_from_default(raw_default)
          default_function = extract_default_function(default_value, raw_default)

          if ActiveRecord.version >= Gem::Version.new('6.1')
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
      end
    end
  end
end
