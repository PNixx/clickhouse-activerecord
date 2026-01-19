module CoreExtensions
  module ActiveRecord
    module QueryMethods

      def reverse_order!
        return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

        orders = order_values.uniq.reject(&:blank?)
        return super unless orders.empty? && !primary_key

        self.order_values = (column_names & %w[date created_at]).map { |c| arel_table[c].desc }
        self
      end

      def build_with_expression_from_value(value, nested = false)
        case value
        when Symbol
          value
        else
          super
        end
      end

    end
  end
end
