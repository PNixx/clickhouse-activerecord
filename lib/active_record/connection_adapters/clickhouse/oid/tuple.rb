# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class Tuple < Type::Value # :nodoc:

          attr_reader :schema_hash

          def self.parse_schema(sql_type)
            elements = sql_type.match(/^Tuple\((.+?)\)$/)[1].split(/,\s*/)
            elements.to_h { |pair| pair.match(/(\w+)\s+(.+)/)[1, 2] }.with_indifferent_access
          end

          def initialize(schema_hash)
            @schema_hash = schema_hash
          end

          def type
            :tuple
          end

          def ==(other)
            super && schema_hash == other.schema_hash
          end

          private

          def cast_value(value)
            value.to_h { |k, v| [k, schema_hash[k].cast(v)] }.with_indifferent_access
          end

        end
      end
    end
  end
end
