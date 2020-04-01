# frozen_string_literal: true

namespace :clickhouse do

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
  task migrate: :load_config do
    Rake::Task['db:migrate'].execute
  end
end
