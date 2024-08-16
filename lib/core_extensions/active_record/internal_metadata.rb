module CoreExtensions
  module ActiveRecord
    module InternalMetadata

      def create_table
        return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
        return if table_exists? || !enabled?

        key_options = connection.internal_string_options_for_primary_key
        table_options = {
          id: false,
          options: 'ReplacingMergeTree(created_at) PARTITION BY key ORDER BY key',
          if_not_exists: true
        }
        full_config = connection.instance_variable_get(:@config) || {}

        if full_config[:distributed_service_tables]
          table_options.merge!(with_distributed: table_name, sharding_key: 'cityHash64(created_at)')

          distributed_suffix = "_#{full_config[:distributed_service_tables_suffix] || 'distributed'}"
        else
          distributed_suffix = ''
        end

        connection.create_table(table_name + distributed_suffix.to_s, **table_options) do |t|
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

        sm = ::Arel::SelectManager.new(arel_table)
        sm.final! if connection.table_options(table_name)[:options] =~ /^ReplacingMergeTree/
        sm.project(::Arel.star)
        sm.where(arel_table[primary_key].eq(::Arel::Nodes::BindParam.new(key)))
        sm.order(arel_table[primary_key].asc)
        sm.limit = 1

        connection.select_one(sm, "#{self.class} Load")
      end

      def connection
        if ::ActiveRecord::version >= Gem::Version.new('7.2')
          @pool.connection
        else
          super
        end
      end
    end
  end
end
