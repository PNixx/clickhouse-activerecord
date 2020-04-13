# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class DateTime < Type::DateTime # :nodoc:

          def serialize(value)
            value = super
            return unless value
            return value.strftime('%Y-%m-%d %H:%M:%S') unless value.acts_like?(:time)

            value.to_time.strftime('%Y-%m-%d %H:%M:%S')
          end

          def type_cast_from_database(value)
            value
          end

          # Type cast a value for schema dumping. This method is private, as we are
          # hoping to remove it entirely.
          def type_cast_for_schema(value) # :nodoc:
            value.inspect
          end

        end
      end
    end
  end
end
