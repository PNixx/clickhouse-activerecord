# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class Map < Type::Value # :nodoc:
          attr_reader :key_type, :value_type

          def initialize(sql_type)
            types = sql_type.match(/Map\((.+),\s?(.+)\)/).captures

            @key_type = cast_type(types.first)
            @value_type = cast_type(types.last)
          end

          def type
            :map
          end

          def cast(value)
            value
          end

          def deserialize(value)
            return value if value.is_a?(Hash)

            JSON.parse(value)
          end

          def serialize(value)
            return '{}' if value.nil?

            "{#{value.map { |key, value| "'#{key}': '#{value}'" }.join(' ')}}"
          end

          private

          def cast_type(type)
            return type if type.nil?

            case type
            when /U?Int\d+/
              :integer
            when /DateTime/
              :datetime
            when /Date/
              :date
            else
              :string
            end
          end
        end
      end
    end
  end
end
