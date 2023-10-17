module CoreExtensions
  module Arel
    module SelectManager

      # @param [Hash] values
      def settings(values)
        @ast.settings = ::Arel::Nodes::Settings.new(values)
        self
      end
    end
  end
end
