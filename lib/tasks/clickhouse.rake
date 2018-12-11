# frozen_string_literal: true

require 'tasks/clickhouse_tasks'

namespace :clickhouse do

  namespace :schema do

    # todo not testing
    desc 'Load database schema'
    task load: :environment do
      ActiveRecord::Base.establish_connection(:"#{Rails.env}_clickhouse")
      load("#{Rails.root}/db/clickhouse_schema.rb")
    end

    desc 'Dump database schema'
    task dump: :environment do
      puts ClickhouseTasks::schema_dump
    end
  end

  namespace :structure do

    desc 'Dump database structure'
    task dump: :environment do
      puts ClickhouseTasks::structure_dump
    end

    desc 'Load database structure (truncates data)'
    task load: :environment do
      puts ClickhouseTasks::structure_load
    end
  end

  namespace :test do

    desc 'Create testing database'
    task create: :environment do
      puts ClickhouseTasks::create(:test)
    end

    desc 'Drop testing database'
    task drop: :environment do
      puts ClickhouseTasks::drop(:test)
    end

    desc 'Purge testing database'
    task purge: :environment do
      puts ClickhouseTasks::purge(:test)
    end

    desc 'Load testing database structure'
    task structure_load: :environment do
      puts ClickhouseTasks::structure_load(:test)
    end

    desc 'Clone testing database structure'
    task clone: :environment do
      puts ClickhouseTasks::test_clone
    end
  end
end
