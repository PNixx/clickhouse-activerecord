# frozen_string_literal: true

require 'bundler/setup'
require 'pry'
require 'active_record'
require 'clickhouse-activerecord'
require 'active_support/testing/stream'

ClickhouseActiverecord.load

FIXTURES_PATH = File.join(File.dirname(__FILE__), 'fixtures')
CLUSTER_NAME = 'test'

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = '.rspec_status'
  config.include ActiveSupport::Testing::Stream

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.around(:each, :migrations) do |example|
    clear_consts
    clear_db

    example.run

    clear_consts
    clear_db
  end
end

ActiveRecord::Base.configurations = HashWithIndifferentAccess.new(
  default: {
    adapter: 'clickhouse',
    host: 'localhost',
    port: 8123,
    database: 'test',
    username: nil,
    password: nil
  }
)

ActiveRecord::Base.establish_connection(:default)

def schema(model)
  model.reset_column_information
  model.columns.each_with_object({}) do |c, h|
    h[c.name] = c
  end
end

def clear_db
  if ActiveRecord::version >= Gem::Version.new('6.1')
    cluster = ActiveRecord::Base.connection_db_config.configuration_hash[:cluster_name]
  else
    cluster = ActiveRecord::Base.connection_config[:cluster_name]
  end
  pattern = if cluster
              normalized_cluster_name = cluster.start_with?('{') ? "'#{cluster}'" : cluster

              "DROP TABLE %s ON CLUSTER #{normalized_cluster_name}"
            else
              'DROP TABLE %s'
            end

  ActiveRecord::Base.connection.tables.each { |table| ActiveRecord::Base.connection.execute(pattern % table) }
rescue ActiveRecord::NoDatabaseError
  # Ignored
end

def clear_consts
  $LOADED_FEATURES.select { |file| file.include? FIXTURES_PATH }.each do |file|
    const = File.basename(file)
                .scan(ActiveRecord::Migration::MigrationFilenameRegexp)[0][1]
                .camelcase
                .safe_constantize

    Object.send(:remove_const, const.to_s) if const
    $LOADED_FEATURES.delete(file)
  end
end
