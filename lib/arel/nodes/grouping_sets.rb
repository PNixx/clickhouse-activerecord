# frozen_string_literal: true

module Arel # :nodoc: all
  module Nodes
    class GroupingSets < Arel::Nodes::Unary

      def initialize(expr)
        super
        @expr = wrap_grouping_sets(expr)
      end

      private

      def wrap_grouping_sets(sets)
        sets.map do |element|
          # See Arel::SelectManager#group
          case element
          when Array
            wrap_grouping_sets(element)
          when String
            ::Arel::Nodes::SqlLiteral.new(element)
          when Symbol
            ::Arel::Nodes::SqlLiteral.new(element.to_s)
          else
            element
          end
        end
      end

    end
  end
end
