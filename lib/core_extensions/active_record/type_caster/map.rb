# frozen_string_literal: true

module CoreExtensions
  module ActiveRecord
    module TypeCaster
      module Map
        def is_view
          if @klass.respond_to?(:is_view)
            @klass.is_view # rails 6.1
          else
            types.is_view # less than 6.1
          end
        end
      end
    end
  end
end
