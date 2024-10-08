module Arel
  module Nodes
    class LimitBy < Arel::Nodes::Unary
      attr_reader :column

      def initialize(limit, column)
        @column = column
        super(limit)
      end
    end
  end
end
