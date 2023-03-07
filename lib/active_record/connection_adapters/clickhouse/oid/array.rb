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
                       when /Decimal/
                         :array_decimal
                       else
                         :string
            end
            case @subtype
            when :array_decimal
              @scale = extract_scale(sql_type)
              @precision = extract_precision(sql_type)
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
                when :array_decimal
                  BigDecimal(apply_scale(value), @precision)
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
                when :array_decimal
                  BigDecimal(apply_scale(value), @precision)
              else
                super
              end
            end
          end

          private
            def apply_scale(value)
              if @scale
                value.round(@scale)
              else
                value
              end
            end

            def extract_scale(sql_type)
              case sql_type
              when /\((\d+)\)/ then 0
              when /\((\d+)(,\s?(\d+))\)/ then $3.to_i
              end
            end

            def extract_precision(sql_type)
              $1.to_i if sql_type =~ /\((\d+)(,\s?\d+)?\)/
            end

        end
      end
    end
  end
end
