module CoreExtensions
  module ActiveRecord
    module InternalMetadata

      def create_table
        return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
        return if !enabled? || table_exists?

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

      def update_entry(connection_or_key, key_or_new_value, new_value = nil)
        if ::ActiveRecord::version >= Gem::Version.new('7.2')
          return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
          create_entry(connection_or_key, key_or_new_value, new_value)
        else
          return super(connection_or_key, key_or_new_value) unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
          create_entry(connection_or_key, key_or_new_value)
        end
      end

      def select_entry(connection_or_key, key = nil)
        if ::ActiveRecord::version >= Gem::Version.new('7.2')
          return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
        else
          key = connection_or_key
          return super(key) unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
        end

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
          @pool.lease_connection
        else
          super
        end
      end
    end
  end
end
