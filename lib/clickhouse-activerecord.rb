# frozen_string_literal: true

require_relative '../core_extensions/active_record/migration/command_recorder'

require 'active_record/connection_adapters/clickhouse_adapter'

require 'clickhouse-activerecord/base'
require 'clickhouse-activerecord/internal_metadata'
require 'clickhouse-activerecord/migrator'
require 'clickhouse-activerecord/relation'
require 'clickhouse-activerecord/schema_migration'
require 'clickhouse-activerecord/type_caster/map'

require 'clickhouse-activerecord/arel/table'

if defined?(Rails::Railtie)
  require 'clickhouse-activerecord/railtie'
  require 'clickhouse-activerecord/tasks'
  ActiveRecord::Tasks::DatabaseTasks.register_task(/clickhouse/, "ClickhouseActiverecord::Tasks")
end

module ClickhouseActiverecord

  def self.load
    ActiveRecord::Base.singleton_class.prepend(ClickhouseActiverecord::Base::ClassMethods)
    ActiveRecord::InternalMetadata.singleton_class.prepend(ClickhouseActiverecord::InternalMetadata::ClassMethods)
    ActiveRecord::Migration::CommandRecorder.include CoreExtensions::ActiveRecord::Migration::CommandRecorder
    ActiveRecord::Migrator.prepend(ClickhouseActiverecord::Migrator)
    ActiveRecord::Relation.prepend(ClickhouseActiverecord::Relation)
    ActiveRecord::SchemaMigration.singleton_class.prepend(ClickhouseActiverecord::SchemaMigration::ClassMethods)
    ActiveRecord::TypeCaster::Map.prepend(ClickhouseActiverecord::TypeCaster::Map)

    ::Arel::Table.prepend(ClickhouseActiverecord::Arel::Table)
  end

end
