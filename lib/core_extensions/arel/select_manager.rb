module CoreExtensions
  module Arel
    module SelectManager

      # @param [Hash] values
      def settings(values)
        @ast.settings = ::Arel::Nodes::Settings.new(values)
        self
      end

      def using(*exprs)
        @ctx.source.right.last.right = ::Arel::Nodes::Using.new(::Arel.sql(exprs.join(',')))
        self
      end
    end
  end
end
