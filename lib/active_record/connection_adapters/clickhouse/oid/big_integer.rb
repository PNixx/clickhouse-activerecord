# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class BigInteger < Type::BigInteger # :nodoc:
          def type
            :big_integer
          end

          def limit
            DEFAULT_LIMIT
          end

          private

          def _limit
            DEFAULT_LIMIT
          end

        end
      end
    end
  end
end
