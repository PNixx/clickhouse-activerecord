# frozen_string_literal: true

require 'bundler/setup'
require 'active_record'
require 'clickhouse-activerecord'
require 'active_support/notifications'
require 'active_support/testing/stream'

ClickhouseActiverecord.load

FIXTURES_PATH = File.join(File.dirname(__FILE__), 'fixtures')

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = '.rspec_status'
  config.include ActiveSupport::Testing::Stream
  config.raise_errors_for_deprecations!

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

  config.filter_run_excluding cluster: !ENV.key?('CLICKHOUSE_CLUSTER')
end

ActiveRecord::Base.configurations = HashWithIndifferentAccess.new(
  default: {
    adapter: 'clickhouse',
    host: 'localhost',
    port: ENV.fetch('CLICKHOUSE_PORT', 8123),
    database: ENV.fetch('CLICKHOUSE_DATABASE', 'test'),
    username: ENV['CLICKHOUSE_USER'],
    password: ENV['CLICKHOUSE_PASSWORD'],
    use_metadata_table: !ENV['CLICKHOUSE_CLUSTER'],
    cluster_name: ENV['CLICKHOUSE_CLUSTER'],
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
  ActiveRecord::Base.connection.tables.each { |table| ActiveRecord::Base.connection.drop_table(table, sync: true) }
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
