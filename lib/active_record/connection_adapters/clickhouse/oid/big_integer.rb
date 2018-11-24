# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class BigInteger < Type::BigInteger # :nodoc:

          DEFAULT_LIMIT = 8

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
