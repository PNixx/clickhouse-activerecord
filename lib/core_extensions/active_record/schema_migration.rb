# frozen_string_literal: true

module CoreExtensions
  module ActiveRecord
    module SchemaMigration
      module ClassMethods
        def create_table
          return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
          return if table_exists?

          version_options = connection.internal_string_options_for_primary_key

          table_options = {
            id: false,
            options: 'ReplacingMergeTree(ver) ORDER BY (version)',
            if_not_exists: true
          }
          full_config = connection.instance_variable_get(:@full_config) || {}
          if full_config[:distributed_service_tables]
            table_options[:with_distributed] = table_name
            table_options[:sharding_key] = 'cityHash64(version)'
            distributed_suffix = "_#{full_config[:distributed_service_tables_suffix] || 'distributed'}"
          end

          connection.create_table("#{table_name}#{distributed_suffix}", **table_options) do |t|
            t.string :version, **version_options
            t.column :active, 'Int8', null: false, default: '1'
            t.datetime :ver, null: false, default: -> { 'now()' }
          end
        end

        def all_versions
          return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

          from("#{table_name} FINAL").where(active: 1).order(:version).pluck(:version)
        end
      end
    end
  end
end
