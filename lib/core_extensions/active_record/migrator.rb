# frozen_string_literal: true

module CoreExtensions
  module ActiveRecord
    module Migrator
      def record_version_state_after_migrating(version)
        if down?
          migrated.delete(version)
          @schema_migration.create!(version: version.to_s, active: 0)
        else
          super
        end
      end
    end
  end
end
