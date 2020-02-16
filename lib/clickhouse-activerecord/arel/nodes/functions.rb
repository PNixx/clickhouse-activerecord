module ClickhouseActiverecord
  module Arel
    module Nodes

      class FunctionZero < ::Arel::Nodes::Function
        attr_reader :funcname
        def initialize(expr, funcname, aliaz = nil)
          super(expr, aliaz)
          @funcname = funcname
        end
      end

      class FunctionOne < ::Arel::Nodes::Function
        attr_reader :argument, :funcname
        def initialize(expr, funcname, argument, aliaz = nil)
          super(expr, aliaz)
          @argument = argument
          @funcname = funcname
        end
      end

      class FunctionTwo < ::Arel::Nodes::Function
        attr_reader :argument1, :argument2, :funcname
        def initialize(expr, funcname, argument1, argument2, aliaz = nil)
          super(expr, aliaz)
          @argument1 = argument1
          @argument2 = argument2
          @funcname = funcname
        end
      end

      class FunctionCountIf < FunctionOne
      end

    end
  end
end