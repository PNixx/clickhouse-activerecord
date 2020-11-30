module ClickhouseActiverecord
  module Arel
    module Nodes

      class LimitBy < ::Arel::Nodes::Unary

        attr_reader :limit, :offset
        def initialize(expr,limit,offset = 0)
          super(expr)
          @limit = limit
          @offset = offset
        end
      end

    end
  end
end