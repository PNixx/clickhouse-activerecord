module ClickhouseActiverecord
  module Arel
    class Table < ::Arel::Table
      def is_view
        type_caster.is_view
      end

      def from
        ClickhouseActiverecord::Arel::SelectManager.new(self)
      end
    end
  end
end
