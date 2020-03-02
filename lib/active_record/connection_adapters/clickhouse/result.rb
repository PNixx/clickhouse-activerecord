module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Result < ::ActiveRecord::Result

        attr_reader :statistics
        def initialize meta, rows, statistics
          super meta.map{|c| c['name']}, convert_types(meta.map{|c| c['type']}, rows)
          @statistics = statistics
        end


        private

        def convert_types types, rows
          rows.collect do |row|
            row.each_with_index.to_a.collect { |value, i|
              parse_value(types[i], value)
            }
          end
        end

        def parse_value(type, value)
          if value
            case type
            when "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64"
              parse_int_value value
            when "Float32", "Float64"
              parse_float_value value
            when /^Decimal/
              parse_decimal_value value
            when "String", "Enum8", "Enum16", "LowCardinality(String)"
              parse_string_value value
            when /FixedString\(\d+\)/
              parse_fixed_string_value value
            when "Date"
              parse_date_value value
            when "DateTime"
              parse_date_time_value value
            when /Array\(/
              parse_array_value value
            else
              raise NotImplementedError, "Cannot parse value of type #{type.inspect}"
            end
          end
        end

        def parse_int_value(value)
          value.to_i
        end

        def parse_float_value(value)
          value.to_f
        end

        def parse_decimal_value(value)
          value.to_d
        end

        def parse_string_value(value)
          value.force_encoding("UTF-8")
        end

        def parse_fixed_string_value(value)
          value.delete("\000").force_encoding("UTF-8")
        end

        def parse_date_value(value)
          if '0000-00-00'==value
            nil
          else
            Date.parse(value)
          end
        end

        def parse_date_time_value(value)
          if '0000-00-00 00:00:00'==value
            nil
          else
            Time.find_zone("UTC").parse(value)
          end
        end

        def parse_array_value(value)
          value
        end


      end
    end
  end
end