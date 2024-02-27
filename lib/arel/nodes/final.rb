module Arel # :nodoc: all
  module Nodes
    class Final < Arel::Nodes::Unary
      delegate :empty?, to: :expr
    end
  end
end
