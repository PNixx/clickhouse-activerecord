# frozen_string_literal: true

require 'active_record/connection_adapters/clickhouse_adapter'

require 'core_extensions/active_record/base'
require 'core_extensions/active_record/internal_metadata'
require 'core_extensions/active_record/migration/command_recorder'
require 'core_extensions/active_record/migrator'
require 'core_extensions/active_record/relation'
require 'core_extensions/active_record/schema_migration'
require 'core_extensions/active_record/type_caster/map'

require 'core_extensions/arel/table'

if defined?(Rails::Railtie)
  require 'clickhouse-activerecord/railtie'
  require 'clickhouse-activerecord/tasks'
  ActiveRecord::Tasks::DatabaseTasks.register_task(/clickhouse/, "ClickhouseActiverecord::Tasks")
end

module ClickhouseActiverecord

  def self.load
    ActiveRecord::Base.singleton_class.prepend(CoreExtensions::ActiveRecord::Base::ClassMethods)
    ActiveRecord::InternalMetadata.singleton_class.prepend(CoreExtensions::ActiveRecord::InternalMetadata::ClassMethods)
    ActiveRecord::Migration::CommandRecorder.include(CoreExtensions::ActiveRecord::Migration::CommandRecorder)
    ActiveRecord::Migrator.prepend(CoreExtensions::ActiveRecord::Migrator)
    ActiveRecord::Relation.prepend(CoreExtensions::ActiveRecord::Relation)
    ActiveRecord::SchemaMigration.singleton_class.prepend(CoreExtensions::ActiveRecord::SchemaMigration::ClassMethods)
    ActiveRecord::TypeCaster::Map.include(CoreExtensions::ActiveRecord::TypeCaster::Map)

    Arel::Table.prepend(CoreExtensions::Arel::Table)
  end

end
