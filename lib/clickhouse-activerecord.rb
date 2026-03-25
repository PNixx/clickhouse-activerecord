# frozen_string_literal: true

require 'active_record/connection_adapters/clickhouse_adapter'

require 'core_extensions/active_record/internal_metadata'
require 'core_extensions/active_record/relation'
require 'core_extensions/active_record/schema_migration'
require 'core_extensions/active_record/migration/command_recorder'
require 'core_extensions/arel/nodes/select_core'
require 'core_extensions/arel/nodes/select_statement'
require 'core_extensions/arel/select_manager'
require 'core_extensions/arel/table'

if defined?(Rails::Railtie)
  require 'clickhouse-activerecord/railtie'
  require 'clickhouse-activerecord/schema'
  require 'clickhouse-activerecord/schema_dumper'
  require 'clickhouse-activerecord/tasks'
  ActiveRecord::Tasks::DatabaseTasks.register_task(/clickhouse/, "ClickhouseActiverecord::Tasks")
end

module ClickhouseActiverecord
  def self.load
    ActiveRecord::InternalMetadata.prepend(CoreExtensions::ActiveRecord::InternalMetadata)
    ActiveRecord::Migration::CommandRecorder.include(CoreExtensions::ActiveRecord::Migration::CommandRecorder)
    ActiveRecord::Relation.prepend(CoreExtensions::ActiveRecord::Relation)
    ActiveRecord::SchemaMigration.prepend(CoreExtensions::ActiveRecord::SchemaMigration)

    Arel::Nodes::SelectCore.prepend(CoreExtensions::Arel::Nodes::SelectCore)
    Arel::Nodes::SelectStatement.prepend(CoreExtensions::Arel::Nodes::SelectStatement)
    Arel::SelectManager.prepend(CoreExtensions::Arel::SelectManager)
    Arel::Table.prepend(CoreExtensions::Arel::Table)

    # Prevent TimeZoneConverter wrapping for Map OID with DateTime subtype.
    # Map(String, DateTime) would crash with "Hash values are not supported"
    # when TimeZoneConverter tries to convert Hash values.
    ActiveRecord::AttributeMethods::TimeZoneConversion::ClassMethods.prepend(Module.new do
      def create_time_zone_conversion_attribute?(name, cast_type)
        return false if cast_type.is_a?(ActiveRecord::ConnectionAdapters::Clickhouse::OID::Map)
        super
      end
    end)
  end
end
