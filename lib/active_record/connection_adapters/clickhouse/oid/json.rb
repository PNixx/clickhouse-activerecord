# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class Json < Type::Value # :nodoc:

          def type
            :json
          end

          def deserialize(value)
            return value if value.nil?
            return value if value.is_a?(::Hash) || value.is_a?(::Array)

            if value.is_a?(::String)
              ::JSON.parse(value)
            else
              super
            end
          rescue ::JSON::ParserError
            nil
          end

          def serialize(value)
            return nil if value.nil?
            ::JSON.generate(value)
          end

          def changed_in_place?(raw_old_value, new_value)
            deserialize(raw_old_value) != new_value
          end

          def accessor
            ActiveRecord::Store::StringKeyedHashAccessor
          end

        end
      end
    end
  end
end
