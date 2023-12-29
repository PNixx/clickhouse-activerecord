# frozen_string_literal: true

require 'bundler/setup'
require 'pry'
require 'active_record'
require 'clickhouse-activerecord'
require 'active_support/notifications'
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
    ActiveRecord::Base.connection.schema_cache.clear!
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
  },
  in_mem: {
    database: ':memory:',
    adapter: 'sqlite3'
  }
)

ActiveRecord::Base.establish_connection(:default)

require_relative 'models/in_mem_base' if ActiveRecord::VERSION::MAJOR >= 6

def schema(model)
  model.reset_column_information
  model.columns.each_with_object({}) do |c, h|
    h[c.name] = c
  end
end

def clear_db
  cluster = ActiveRecord::Base.connection_db_config.configuration_hash[:cluster_name]
  pattern =
    if cluster
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

class SqlCapture
  def initialize(&block)
    @block = block
  end

  def captured
    trap = ->(_name, _started, _finished, _unique_id, payload) { store_captured(payload[:sql]) }
    ActiveSupport::Notifications.subscribed(trap, 'sql.active_record') { @block.call }
    @captured
  end

  private

  def store_captured(sql)
    @captured = sql
  end
end
