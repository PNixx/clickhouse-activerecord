module ClickhouseActiverecord
  module Arel
    class Table < ::Arel::Table
      def is_view
        type_caster.is_view
      end
    end
  end
end
