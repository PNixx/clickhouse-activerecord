module CoreExtensions
  module Arel
    module Table
      def is_view
        type_caster.is_view
      end
    end
  end
end
