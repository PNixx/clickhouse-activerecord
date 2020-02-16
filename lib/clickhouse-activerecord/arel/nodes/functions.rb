module ClickhouseActiverecord
  module Arel
    module Nodes

      class Function < ::Arel::Nodes::Function
        attr_reader :condition, :funcname
        def initialize(expr, funcname, condition, aliaz = nil)
          super(expr, aliaz)
          @condition = condition
          @funcname = funcname
        end
      end

      class FunctionZero < Function
      end

      class FunctionOne < Function
      end

    end
  end
end