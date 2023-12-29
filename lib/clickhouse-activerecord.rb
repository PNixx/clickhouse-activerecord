# frozen_string_literal: true

require 'active_record/connection_adapters/clickhouse_adapter'

require 'core_extensions/active_record/base'
require 'core_extensions/active_record/internal_metadata'
require 'core_extensions/active_record/migration/command_recorder'
require 'core_extensions/active_record/migrator'
require 'core_extensions/active_record/relation'
require 'core_extensions/active_record/schema_migration'
require 'core_extensions/active_record/type_caster/map'

require 'core_extensions/arel/nodes/select_core'
require 'core_extensions/arel/nodes/select_statement'
require 'core_extensions/arel/select_manager'
require 'core_extensions/arel/table'

if defined?(Rails::Railtie)
  require 'clickhouse-activerecord/railtie'
  require 'clickhouse-activerecord/tasks'
  ActiveRecord::Tasks::DatabaseTasks.register_task(/clickhouse/, "ClickhouseActiverecord::Tasks")
end

module ClickhouseActiverecord
  class << self
    def load
      ActiveRecord::Base.singleton_class.prepend(CoreExtensions::ActiveRecord::Base::ClassMethods)
      prepend_for_rails_version ActiveRecord::InternalMetadata, CoreExtensions::ActiveRecord::InternalMetadata
      ActiveRecord::Migration::CommandRecorder.include(CoreExtensions::ActiveRecord::Migration::CommandRecorder)
      ActiveRecord::Migrator.prepend(CoreExtensions::ActiveRecord::Migrator)
      ActiveRecord::Relation.prepend(CoreExtensions::ActiveRecord::Relation)
      prepend_for_rails_version ActiveRecord::SchemaMigration, CoreExtensions::ActiveRecord::SchemaMigration
      ActiveRecord::TypeCaster::Map.include(CoreExtensions::ActiveRecord::TypeCaster::Map)

      Arel::Nodes::SelectCore.prepend(CoreExtensions::Arel::Nodes::SelectCore)
      Arel::Nodes::SelectStatement.prepend(CoreExtensions::Arel::Nodes::SelectStatement)
      Arel::SelectManager.prepend(CoreExtensions::Arel::SelectManager)
      Arel::Table.prepend(CoreExtensions::Arel::Table)
    end

    private

    def prepend_for_rails_version(ar_module, ch_module)
      receiver = ar_module
      receiver = receiver.singleton_class if ActiveRecord.version < Gem::Version.new('7.1')
      receiver.prepend ch_module
    end
  end
end
