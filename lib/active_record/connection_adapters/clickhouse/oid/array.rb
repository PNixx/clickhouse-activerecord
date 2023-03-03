# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class Array < Type::Value # :nodoc:

          def initialize(sql_type)
            @subtype = case sql_type
                       when /U?Int\d+/
                         :integer
                       when /DateTime/
                         :array_datetime
                       when /Date/
                         :array_date
                       else
                         :string
            end
          end

          def type
            @subtype
          end

          def deserialize(value)
            if value.is_a?(::Array)
              value.map { |item| deserialize(item) }
            else
              return value if value.nil?
              case @subtype
                when :integer
                  value.to_i
                when :array_datetime
                  ::DateTime.parse(value)
                when :array_date
                  ::Date.parse(value)
              else
                super
              end
            end
          end

          def serialize(value)
            if value.is_a?(::Array)
              value.map { |item| serialize(item) }
            else
              return value if value.nil?
              case @subtype
                when :integer
                  value.to_i
                when :array_datetime
                  ::DateTime.parse(value)
                when :array_date
                  ::Date.parse(value)
              else
                super
              end
            end
          end

        end
      end
    end
  end
end
