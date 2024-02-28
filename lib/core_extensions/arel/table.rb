module CoreExtensions
  module Arel
    module Table
      attr_accessor :final

      def is_view
        type_caster.is_view
      end
    end
  end
end
