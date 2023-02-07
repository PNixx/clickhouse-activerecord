# frozen_string_literal: true

module CoreExtensions
  module Arel
    module SelectManager
      def settings(values)
        @ast.settings = ::Arel::Nodes::Settings.new(values)
        self
      end
    end
  end
end
