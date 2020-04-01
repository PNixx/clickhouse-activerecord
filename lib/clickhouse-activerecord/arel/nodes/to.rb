module ClickhouseActiverecord
  module Arel
    module Nodes

      class To < ::Arel::Nodes::Function

        attr_reader :type
        def initialize(expr, type, aliaz = nil)
          super(expr, aliaz)
          @type = type
        end
      end

    end
  end
end