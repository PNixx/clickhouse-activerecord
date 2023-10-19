require 'active_record/migration'

module ClickhouseActiverecord

  class SchemaMigration < ::ActiveRecord::SchemaMigration
    def create_table
      return if table_exists?

      version_options = connection.internal_string_options_for_primary_key
      table_options = {
        id: false, options: 'ReplacingMergeTree(ver) ORDER BY (version)', if_not_exists: true
      }
      full_config = connection.instance_variable_get(:@full_config) || {}

      if full_config[:distributed_service_tables]
        table_options.merge!(with_distributed: table_name, sharding_key: 'cityHash64(version)')

        distributed_suffix = "_#{full_config[:distributed_service_tables_suffix] || 'distributed'}"
      end

      connection.create_table(table_name + distributed_suffix.to_s, **table_options) do |t|
        t.string :version, **version_options
        t.column :active, 'Int8', null: false, default: '1'
        t.datetime :ver, null: false, default: -> { 'now()' }
      end
    end

    def all_versions
      final.where(active: 1).order(:version).pluck(:version)
    end
  end

  class InternalMetadata < ::ActiveRecord::InternalMetadata

    def create_table
      return if table_exists? || !enabled?

      key_options = connection.internal_string_options_for_primary_key
      table_options = {
        id: false,
        options: connection.adapter_name.downcase == 'clickhouse' ? 'ReplacingMergeTree(created_at) PARTITION BY key ORDER BY key' : '',
        if_not_exists: true
      }
      full_config = connection.instance_variable_get(:@full_config) || {}

      if full_config[:distributed_service_tables]
        table_options.merge!(with_distributed: table_name, sharding_key: 'cityHash64(created_at)')

        distributed_suffix = "_#{full_config[:distributed_service_tables_suffix] || 'distributed'}"
      end

      connection.create_table(table_name + distributed_suffix.to_s, **table_options) do |t|
        t.string :key, **key_options
        t.string :value
        t.timestamps
      end
    end

    private

    def update_entry(key, new_value)
      create_entry(key, new_value)
    end

    def select_entry(key)
      table = arel_table.dup
      table.final = true
      sm = Arel::SelectManager.new(table)
      sm.project(Arel::Nodes::SqlLiteral.new("*"))
      sm.where(table[primary_key].eq(Arel::Nodes::BindParam.new(key)))
      sm.order(table[primary_key].asc)
      sm.limit = 1

      connection.select_all(sm, "#{self.class} Load").first
    end
  end

  class MigrationContext < ::ActiveRecord::MigrationContext #:nodoc:

    def up(target_version = nil)
      selected_migrations = if block_given?
        migrations.select { |m| yield m }
      else
        migrations
      end

      ClickhouseActiverecord::Migrator.new(:up, selected_migrations, schema_migration, internal_metadata, target_version).migrate
    end

    def down(target_version = nil)
      selected_migrations = if block_given?
        migrations.select { |m| yield m }
      else
        migrations
      end

      ClickhouseActiverecord::Migrator.new(:down, selected_migrations, schema_migration, internal_metadata, target_version).migrate
    end

    def get_all_versions
      if schema_migration.table_exists?
        schema_migration.all_versions.map(&:to_i)
      else
        []
      end
    end

  end

  class Migrator < ::ActiveRecord::Migrator

    def record_version_state_after_migrating(version)
      if down?
        migrated.delete(version)
        @schema_migration.create!(version: version.to_s, active: 0)
      else
        super
      end
    end

  end
end
