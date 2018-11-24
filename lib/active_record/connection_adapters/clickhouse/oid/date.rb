# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class Date < Type::Date # :nodoc:

          def type_cast_from_database(value)
            value
          end

          # Type cast a value for schema dumping. This method is private, as we are
          # hoping to remove it entirely.
          def type_cast_for_schema(value) # :nodoc:
            case value
              when 'toDate(now()', 'CAST(now() AS Date)'
                'now()'.inspect
              else
                value.inspect
            end
          end

        end
      end
    end
  end
end
