# frozen_string_literal: true

namespace :clickhouse do
  task prepare_schema_migration_table: :environment do
    ClickhouseActiverecord::SchemaMigration.create_table unless ENV['simple'] || ARGV.any? { |a| a.include?('--simple') }
  end

  task prepare_internal_metadata_table: :environment do
    ClickhouseActiverecord::InternalMetadata.create_table unless ENV['simple'] || ARGV.any? { |a| a.include?('--simple') }
  end

  task load_config: :environment do
    ENV['SCHEMA'] = 'db/clickhouse_schema.rb'
    ActiveRecord::Migrator.migrations_paths = %w[db/migrate_clickhouse]
    ActiveRecord::Base.establish_connection(:clickhouse)
  end

  namespace :schema do
    # TODO: not testing
    desc 'Load database schema'
    task load: %i[load_config prepare_internal_metadata_table] do
      simple = ENV['simple'] || ARGV.any? { |a| a.include?('--simple') } ? '_simple' : nil
      ClickhouseActiverecord::SchemaMigration.drop_table
      load(Rails.root.join("db/clickhouse_schema#{simple}.rb"))
    end

    desc 'Dump database schema'
    task dump: :environment do |_, args|
      simple = ENV['simple'] || args[:simple] || ARGV.any? { |a| a.include?('--simple') } ? '_simple' : nil
      filename = Rails.root.join("db/clickhouse_schema#{simple}.rb")
      File.open(filename, 'w:utf-8') do |file|
        ActiveRecord::Base.establish_connection(:clickhouse)
        ClickhouseActiverecord::SchemaDumper.dump(ActiveRecord::Base.connection, file, ActiveRecord::Base, simple.present?)
      end
    end
  end

  namespace :structure do
    desc 'Load database structure'
    task load: [:load_config, 'db:check_protected_environments'] do
      config = ActiveRecord::Base.configurations.configs_for(env_name: Rails.env, name: 'clickhouse')
      ClickhouseActiverecord::Tasks.new(config).structure_load(Rails.root.join('db/clickhouse_structure.sql'))
    end

    desc 'Dump database structure'
    task dump: [:load_config, 'db:check_protected_environments'] do
      config = ActiveRecord::Base.configurations.configs_for(env_name: Rails.env, name: 'clickhouse')
      ClickhouseActiverecord::Tasks.new(config).structure_dump(Rails.root.join('db/clickhouse_structure.sql'))
    end
  end

  desc 'Creates the database from DATABASE_URL or config/database.yml'
  task create: [:load_config] do
    config = ActiveRecord::Base.configurations.configs_for(env_name: Rails.env, name: 'clickhouse')
    ActiveRecord::Tasks::DatabaseTasks.create(config)
  end

  desc 'Drops the database from DATABASE_URL or config/database.yml'
  task drop: [:load_config, 'db:check_protected_environments'] do
    config = ActiveRecord::Base.configurations.configs_for(env_name: Rails.env, name: 'clickhouse')
    ActiveRecord::Tasks::DatabaseTasks.drop(config)
  end

  desc 'Empty the database from DATABASE_URL or config/database.yml'
  task purge: [:load_config, 'db:check_protected_environments'] do
    config = ActiveRecord::Base.configurations.configs_for(env_name: Rails.env, name: 'clickhouse')
    ActiveRecord::Tasks::DatabaseTasks.purge(config)
  end

  # desc 'Resets your database using your migrations for the current environment'
  task reset: :load_config do
    Rake::Task['clickhouse:purge'].execute
    Rake::Task['clickhouse:migrate'].execute
  end

  desc 'Migrate the clickhouse database'
  task migrate: %i[load_config prepare_schema_migration_table prepare_internal_metadata_table] do
    Rake::Task['db:migrate'].execute
    if File.exists? "#{Rails.root}/db/clickhouse_schema_simple.rb"
      Rake::Task['clickhouse:schema:dump'].execute(simple: true)
    end
  end

  desc 'Rollback the clickhouse database'
  task rollback: %i[load_config prepare_schema_migration_table prepare_internal_metadata_table] do
    Rake::Task['db:rollback'].execute
  end
end
