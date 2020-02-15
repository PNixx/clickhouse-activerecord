module ClickhouseActiverecord
  module Arel
    module Nodes

      class CountIf < ::Arel::Nodes::Function
        attr_reader :condition
        def initialize(expr, condition, aliaz = nil)
          super(expr, aliaz)
          @condition = condition
        end
      end

      class SumIf < ::Arel::Nodes::Function
        attr_reader :condition
        def initialize(expr, condition, aliaz = nil)
          super(expr, aliaz)
          @condition = condition
        end
      end

    end
  end
end