# frozen_string_literal: true

module CoreExtensions
  module ActiveRecord
    module InternalMetadata
      def create_table
        return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

        return if table_exists? || !enabled?

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

      private

      def update_entry(key, new_value)
        return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

        create_entry(key, new_value)
      end

      def select_entry(key)
        return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

        table = arel_table.dup
        sm = ::Arel::SelectManager.new(table)
        sm.final!
        sm.project(::Arel::Nodes::SqlLiteral.new("*"))
        sm.where(table[primary_key].eq(::Arel::Nodes::BindParam.new(key)))
        sm.order(table[primary_key].asc)
        sm.limit = 1

        connection.select_all(sm, "#{self.class} Load").first
      end
    end
  end
end
