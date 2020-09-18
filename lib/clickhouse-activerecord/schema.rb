# frozen_string_literal: true

module ClickhouseActiverecord

  class Schema < ::ActiveRecord::Schema

    def define(info, &block) # :nodoc:
      instance_eval(&block)

      if info[:version].present?
        connection.schema_migration.create_table
        connection.assume_migrated_upto_version(info[:version], ClickhouseActiverecord::Migrator.migrations_paths)
      end

      ClickhouseActiverecord::InternalMetadata.create_table
      ClickhouseActiverecord::InternalMetadata[:environment] = connection.migration_context.current_environment
    end
  end
end
