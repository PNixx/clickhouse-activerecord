# frozen_string_literal: true

module CoreExtensions
  module ActiveRecord
    module InternalMetadata
      module ClassMethods
        def []=(key, value)
          row = final.find_by(key: key)
          if row.nil? || row.value != value
            create!(key: key, value: value)
          end
        end

        def [](key)
          final.where(key: key).pluck(:value).first
        end

        def create_table
          return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
          return if table_exists?

          key_options = connection.internal_string_options_for_primary_key

          table_options = {
            id: false,
            options: ('ReplacingMergeTree(created_at) PARTITION BY key ORDER BY key' if connection.adapter_name.downcase == 'clickhouse'),
            if_not_exists: true
          }
          full_config = connection.instance_variable_get(:@full_config) || {}
          if full_config[:distributed_service_tables]
            table_options[:with_distributed] = table_name
            table_options[:sharding_key] = 'cityHash64(created_at)'
            distributed_suffix = "_#{full_config[:distributed_service_tables_suffix] || 'distributed'}"
          end

          connection.create_table("#{table_name}#{distributed_suffix}", **table_options) do |t|
            t.string :key, **key_options
            t.string :value
            t.timestamps
          end
        end
      end
    end
  end
end
