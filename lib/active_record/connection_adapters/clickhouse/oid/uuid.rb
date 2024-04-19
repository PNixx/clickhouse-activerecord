# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class Uuid < Type::Value # :nodoc:
          ACCEPTABLE_UUID = /\A[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}\z/i

          alias :serialize :deserialize

          def type
            :uuid
          end

          def changed?(old_value, new_value, _)
            old_value.class != new_value.class ||
              new_value && old_value.casecmp(new_value) != 0
          end

          def changed_in_place?(raw_old_value, new_value)
            raw_old_value.class != new_value.class ||
              new_value && raw_old_value.casecmp(new_value) != 0
          end

          private

          def cast_value(value)
            casted = value.to_s
            casted if casted.match?(ACCEPTABLE_UUID)
          end
        end
      end
    end
  end
end
