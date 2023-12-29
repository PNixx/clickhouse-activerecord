# frozen_string_literal: true

module CoreExtensions
  module ActiveRecord
    module Migrator
      def record_version_state_after_migrating(version)
        return super unless @schema_migration.connection.adapter_name == "Clickhouse"
        return super if up?

        migrated.delete(version)
        if ::ActiveRecord.version < Gem::Version.new('7.1')
          @schema_migration.create!(version: version.to_s, active: 0)
        else
          @schema_migration.delete_version(version.to_s)
        end
      end
    end
  end
end
