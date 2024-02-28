module CoreExtensions
  module ActiveRecord
    module Relation
      def reverse_order!
        return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

        orders = order_values.uniq.reject(&:blank?)
        return super unless orders.empty? && !primary_key

        self.order_values = (column_names & %w[date created_at]).map { |c| arel_table[c].desc }
        self
      end

      # @param [Hash] opts
      def settings(**opts)
        check_command('SETTINGS')
        @values[:settings] = (@values[:settings] || {}).merge opts
        self
      end

      # @param [Boolean] final
      def final(final = true)
        check_command('FINAL')
        @table = @table.dup
        @table.final = final
        self
      end

      private

      def check_command(cmd)
        raise ::ActiveRecord::ActiveRecordError, cmd + ' is a ClickHouse specific query clause' unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
      end

      def build_arel(aliases = nil)
        arel = super

        arel.settings(@values[:settings]) if @values[:settings].present?

        arel
      end
    end
  end
end
