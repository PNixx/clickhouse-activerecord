require 'active_record/migration'

module ClickhouseActiverecord

  class SchemaMigration < ::ActiveRecord::SchemaMigration
    class << self

      def create_table
        unless table_exists?
          version_options = connection.internal_string_options_for_primary_key

          connection.create_table(table_name, id: false, options: 'ReplacingMergeTree(ver) PARTITION BY version ORDER BY (version)', if_not_exists: true) do |t|
            t.string :version, version_options
            t.column :active, 'Int8', null: false, default: '1'
            t.datetime :ver, null: false, default: -> { 'now()' }
          end
        end
      end

      def all_versions
        from("#{table_name} FINAL").where(active: 1).order(:version).pluck(:version)
      end
    end
  end

  class InternalMetadata < ::ActiveRecord::InternalMetadata
    class << self
      def create_table
        unless table_exists?
          key_options = connection.internal_string_options_for_primary_key

          connection.create_table(table_name, id: false, options: 'MergeTree() PARTITION BY toDate(created_at) ORDER BY (created_at)', if_not_exists: true) do |t|
            t.string :key, key_options
            t.string :value
            t.timestamps
          end
        end
      end
    end
  end

  class MigrationContext < ::ActiveRecord::MigrationContext #:nodoc:
    attr_reader :migrations_paths, :schema_migration

    def initialize(migrations_paths, schema_migration)
      @migrations_paths = migrations_paths
      @schema_migration = schema_migration
    end

    def down(target_version = nil)
      selected_migrations = if block_given?
        migrations.select { |m| yield m }
      else
        migrations
      end

      ClickhouseActiverecord::Migrator.new(:down, selected_migrations, schema_migration, target_version).migrate
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

    def initialize(direction, migrations, schema_migration, target_version = nil)
      @direction         = direction
      @target_version    = target_version
      @migrated_versions = nil
      @migrations        = migrations
      @schema_migration  = schema_migration

      validate(@migrations)

      @schema_migration.create_table
      ClickhouseActiverecord::InternalMetadata.create_table
    end

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
