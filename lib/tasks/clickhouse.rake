# frozen_string_literal: true

namespace :clickhouse do

  task prepare_schema_migration_table: :environment do
    cluster, database, replica = ActiveRecord::Base.connection_config.values_at(:cluster, :database, :replica)
    return if cluster.nil?

    connection = ActiveRecord::Base.connection
    key_options = connection.internal_string_options_for_primary_key
    block = Proc.new do |t|
      t.string :version, key_options
    end
    distributed_table_name = ".#{ActiveRecord::SchemaMigration.table_name}_distributed"
    unless connection.table_exists?(distributed_table_name)
      options = { id: false }
      if replica
        shard = replica.is_a?(String) ? replica : '{shard}'
        options[:options] = <<-SQL
          ReplicatedMergeTree('/clickhouse/tables/{cluster}/#{shard}/#{database}.`#{distributed_table_name}`', '{replica}')
          PARTITION BY version ORDER BY (version) SETTINGS index_granularity = 8192
        SQL
      end
      connection.create_table("`#{distributed_table_name}`", options, &block)
    end
    unless connection.table_exists?(ActiveRecord::SchemaMigration.table_name)
      connection.create_table(
        ActiveRecord::SchemaMigration.table_name,
        id: false,
        options: "Distributed(#{cluster},#{database},`#{distributed_table_name}`,sipHash64(version))",
        &block
      )
    end
  end

  task prepare_internal_metadata_table: :environment do
    cluster, database, replica = ActiveRecord::Base.connection_config.values_at(:cluster, :database, :replica)
    return if cluster.nil?

    connection = ActiveRecord::Base.connection
    key_options = connection.internal_string_options_for_primary_key
    block = Proc.new do |t|
      t.string :key, key_options
      t.string :value
      t.timestamps
    end
    distributed_table_name = ".#{ActiveRecord::InternalMetadata.table_name}_distributed"
    unless connection.table_exists?(distributed_table_name)
      options = { id: false }
      if replica
        shard = replica.is_a?(String) ? replica : '{shard}'
        options[:options] = <<-SQL
          ReplicatedMergeTree('/clickhouse/tables/{cluster}/#{shard}/#{database}.`#{distributed_table_name}`', '{replica}')
          PARTITION BY toDate(created_at) ORDER BY (created_at) SETTINGS index_granularity = 8192
        SQL
      end
      connection.create_table("`#{distributed_table_name}`", options, &block)
    end
    unless connection.table_exists?(ActiveRecord::InternalMetadata.table_name)
      connection.create_table(
        ActiveRecord::InternalMetadata.table_name,
        id: false,
        options: "Distributed(#{cluster},#{database},`#{distributed_table_name}`,sipHash64(created_at))",
        &block
      )
    end
  end

  task load_config: :environment do
    ENV['SCHEMA'] = "db/clickhouse_schema.rb"
    ActiveRecord::Migrator.migrations_paths = ["db/migrate_clickhouse"]
    ActiveRecord::Base.establish_connection(:"#{Rails.env}_clickhouse")
  end

  namespace :schema do

    # todo not testing
    desc 'Load database schema'
    task load: :load_config do
      load("#{Rails.root}/db/clickhouse_schema.rb")
    end

    desc 'Dump database schema'
    task dump: :environment do
      filename = "#{Rails.root}/db/clickhouse_schema.rb"
      File.open(filename, 'w:utf-8') do |file|
        ActiveRecord::Base.establish_connection(:"#{Rails.env}_clickhouse")
        ClickhouseActiverecord::SchemaDumper.dump(ActiveRecord::Base.connection, file)
      end
    end

  end

  namespace :structure do
    desc 'Load database structure'
    task load: [:load_config, 'db:check_protected_environments'] do
      ClickhouseActiverecord::Tasks.new(ActiveRecord::Base.configurations["#{Rails.env}_clickhouse"]).structure_load("#{Rails.root}/db/clickhouse_structure.sql")
    end

    desc 'Dump database structure'
    task dump: [:load_config, 'db:check_protected_environments'] do
      ClickhouseActiverecord::Tasks.new(ActiveRecord::Base.configurations["#{Rails.env}_clickhouse"]).structure_dump("#{Rails.root}/db/clickhouse_structure.sql")
    end
  end

  desc 'Creates the database from DATABASE_URL or config/database.yml'
  task create: [:load_config] do
    ActiveRecord::Tasks::DatabaseTasks.create(ActiveRecord::Base.configurations["#{Rails.env}_clickhouse"])
  end

  desc 'Drops the database from DATABASE_URL or config/database.yml'
  task drop: [:load_config, 'db:check_protected_environments'] do
    ActiveRecord::Tasks::DatabaseTasks.drop(ActiveRecord::Base.configurations["#{Rails.env}_clickhouse"])
  end

  desc 'Empty the database from DATABASE_URL or config/database.yml'
  task purge: [:load_config, 'db:check_protected_environments'] do
    ActiveRecord::Tasks::DatabaseTasks.purge(ActiveRecord::Base.configurations["#{Rails.env}_clickhouse"])
  end

  # desc 'Resets your database using your migrations for the current environment'
  task reset: :load_config do
    Rake::Task['clickhouse:purge'].execute
    Rake::Task['clickhouse:migrate'].execute
  end

  desc 'Migrate the clickhouse database'
  task migrate: [:load_config, :prepare_schema_migration_table, :prepare_internal_metadata_table] do
    Rake::Task['db:migrate'].execute
  end
end
