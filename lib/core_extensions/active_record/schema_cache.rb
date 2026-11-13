# frozen_string_literal: true

module CoreExtensions
  module ActiveRecord
    module SchemaCache
      # Filling the whole cache (for example `db:schema:cache:dump`) asks for the columns
      # and primary keys of every table. For ClickHouse, read them all in one request
      # instead of two requests per table. Other adapters are not affected.
      #
      # Rails 7.1 passes a connection to `add_all`. Rails 7.2 and later pass a pool.
      def add_all(pool_or_connection)
        if pool_or_connection.is_a?(::ActiveRecord::ConnectionAdapters::AbstractAdapter)
          with_bulk_schema_load_on(pool_or_connection) { super(pool_or_connection) }
        else
          pool_or_connection.with_connection do |connection|
            with_bulk_schema_load_on(connection) { super(pool_or_connection) }
          end
        end
      end

      private

      def with_bulk_schema_load_on(connection, &block)
        return yield unless connection.respond_to?(:with_bulk_schema_load)

        connection.with_bulk_schema_load(&block)
      end
    end
  end
end
