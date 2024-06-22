# frozen_string_literal: true

namespace :clickhouse do
  task prepare_schema_migration_table: :environment do
    connection = ActiveRecord::Tasks::DatabaseTasks.migration_connection
    connection.schema_migration.create_table unless ENV['simple'] || ARGV.any? { |a| a.include?('--simple') }
  end

  task prepare_internal_metadata_table: :environment do
    connection = ActiveRecord::Tasks::DatabaseTasks.migration_connection
    connection.internal_metadata.create_table unless ENV['simple'] || ARGV.any? { |a| a.include?('--simple') }
  end

  namespace :schema do
    # TODO: deprecated
    desc 'Load database schema'
    task load: %i[prepare_internal_metadata_table] do
      puts 'Warning: `rake clickhouse:schema:load` is deprecated! Use `rake db:schema:load:clickhouse` instead'
      simple = ENV['simple'] || ARGV.any? { |a| a.include?('--simple') } ? '_simple' : nil
      ActiveRecord::Base.establish_connection(:clickhouse)
      connection = ActiveRecord::Tasks::DatabaseTasks.migration_connection
      connection.schema_migration.drop_table
      load(Rails.root.join("db/clickhouse_schema#{simple}.rb"))
    end

    # TODO: deprecated
    desc 'Dump database schema'
    task dump: :environment do |_, args|
      puts 'Warning: `rake clickhouse:schema:dump` is deprecated! Use `rake db:schema:dump:clickhouse` instead'
      simple = ENV['simple'] || args[:simple] || ARGV.any? { |a| a.include?('--simple') } ? '_simple' : nil
      filename = Rails.root.join("db/clickhouse_schema#{simple}.rb")
      File.open(filename, 'w:utf-8') do |file|
        ActiveRecord::Base.establish_connection(:clickhouse)
        ClickhouseActiverecord::SchemaDumper.dump(ActiveRecord::Base.connection, file, ActiveRecord::Base, simple.present?)
      end
    end
  end

  namespace :structure do
    config = ActiveRecord::Base.configurations.configs_for(env_name: Rails.env, name: 'clickhouse')

    desc 'Load database structure'
    task load: ['db:check_protected_environments'] do
      ClickhouseActiverecord::Tasks.new(config).structure_load(Rails.root.join('db/clickhouse_structure.sql'))
    end

    desc 'Dump database structure'
    task dump: ['db:check_protected_environments'] do
      ClickhouseActiverecord::Tasks.new(config).structure_dump(Rails.root.join('db/clickhouse_structure.sql'))
    end
  end

  desc 'Creates the database from DATABASE_URL or config/database.yml'
  task create: [] do
    puts 'Warning: `rake clickhouse:create` is deprecated! Use `rake db:create:clickhouse` instead'
  end

  desc 'Drops the database from DATABASE_URL or config/database.yml'
  task drop: ['db:check_protected_environments'] do
    puts 'Warning: `rake clickhouse:drop` is deprecated! Use `rake db:drop:clickhouse` instead'
  end

  desc 'Empty the database from DATABASE_URL or config/database.yml'
  task purge: ['db:check_protected_environments'] do
    puts 'Warning: `rake clickhouse:purge` is deprecated! Use `rake db:reset:clickhouse` instead'
  end

  # desc 'Resets your database using your migrations for the current environment'
  task :reset  do
    puts 'Warning: `rake clickhouse:reset` is deprecated! Use `rake db:reset:clickhouse` instead'
  end

  desc 'Migrate the clickhouse database'
  task migrate: %i[prepare_schema_migration_table prepare_internal_metadata_table] do
    puts 'Warning: `rake clickhouse:migrate` is deprecated! Use `rake db:migrate:clickhouse` instead'
    Rake::Task['db:migrate:clickhouse'].execute
    if File.exist? "#{Rails.root}/db/clickhouse_schema_simple.rb"
      Rake::Task['clickhouse:schema:dump'].execute(simple: true)
    end
  end

  desc 'Rollback the clickhouse database'
  task rollback: %i[prepare_schema_migration_table prepare_internal_metadata_table] do
    puts 'Warning: `rake clickhouse:rollback` is deprecated! Use `rake db:rollback:clickhouse` instead'
    Rake::Task['db:rollback:clickhouse'].execute
    if File.exist? "#{Rails.root}/db/clickhouse_schema_simple.rb"
      Rake::Task['clickhouse:schema:dump'].execute(simple: true)
    end
  end
end
