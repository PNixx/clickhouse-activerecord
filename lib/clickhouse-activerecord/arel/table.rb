# frozen_string_literal: true

module ClickhouseActiverecord
  module Arel
    module Table
      def is_view
        type_caster.is_view
      end
    end
  end
end
