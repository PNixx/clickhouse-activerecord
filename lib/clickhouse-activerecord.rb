# frozen_string_literal: true

require 'active_record/connection_adapters/clickhouse_adapter'

require 'core_extensions/active_record/relation'

require_relative '../core_extensions/active_record/migration/command_recorder'
ActiveRecord::Migration::CommandRecorder.include CoreExtensions::ActiveRecord::Migration::CommandRecorder

if defined?(Rails::Railtie)
  require 'clickhouse-activerecord/railtie'
  require 'clickhouse-activerecord/schema'
  require 'clickhouse-activerecord/schema_dumper'
  require 'clickhouse-activerecord/tasks'
  ActiveRecord::Tasks::DatabaseTasks.register_task(/clickhouse/, "ClickhouseActiverecord::Tasks")
end

module ClickhouseActiverecord
  def self.load
    ActiveRecord::Relation.prepend(CoreExtensions::ActiveRecord::Relation)
  end
end
