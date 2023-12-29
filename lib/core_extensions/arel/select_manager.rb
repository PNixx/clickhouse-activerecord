# frozen_string_literal: true

module CoreExtensions
  module Arel
    module SelectManager
      def final!
        @ctx.final = true
        self
      end

      def settings(values)
        @ast.settings = ::Arel::Nodes::Settings.new(values)
        self
      end
    end
  end
end
