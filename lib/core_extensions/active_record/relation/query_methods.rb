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

      def build_with_value_from_hash(hash)
        return super if ::ActiveRecord::version >= Gem::Version.new('7.2')

        # Redefine for ActiveRecord < 7.2
        hash.map do |name, value|
          expression =
            case value
            when ::Arel::Nodes::SqlLiteral then ::Arel::Nodes::Grouping.new(value)
            when ::ActiveRecord::Relation then value.arel
            when ::Arel::SelectManager then value
            when Symbol then value
            else
              raise ArgumentError, "Unsupported argument type: `#{value}` #{value.class}"
            end
          ::Arel::Nodes::TableAlias.new(expression, name)
        end
      end

      def build_with_expression_from_value(value, nested = false)
        case value
        when Symbol then value
        else
          super
        end
      end

    end
  end
end
