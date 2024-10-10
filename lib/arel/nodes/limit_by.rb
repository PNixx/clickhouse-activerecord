module Arel # :nodoc: all
  module Nodes
    class LimitBy < Arel::Nodes::Unary
      attr_reader :column

      def initialize(limit, column)
        raise ArgumentError, 'Limit should be an integer' unless limit.is_a?(Integer)
        raise ArgumentError, 'Limit should be a positive integer' unless limit >= 0
        raise ArgumentError, 'Column should be a Symbol or String' unless column.is_a?(String) || column.is_a?(Symbol)

        super(limit)
      end

      private

      def sanitize(value)
        value 
      end
    end
  end
end
