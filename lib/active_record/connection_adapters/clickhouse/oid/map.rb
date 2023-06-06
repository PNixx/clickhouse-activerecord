# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class Map < Type::Value # :nodoc:

          def initialize(sql_type)
            @subtype = case sql_type
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

          def type
            @subtype
          end

          def deserialize(value)
            if value.is_a?(::Hash)
              value.map { |k,v| 
                [k, deserialize(v)]
              }.to_h
            else
              return value if value.nil?
              case @subtype
                when :integer
                  value.to_i
                when :datetime
                  ::DateTime.parse(value)
                when :date
                  ::Date.parse(value)
              else
                super
              end
            end
          end

          def serialize(value)
            if value.is_a?(::Hash)
              value.map { |k,v| [k.to_s, serialize(v)] }.to_h.to_json.gsub('"', '\'')
            else
              return value if value.nil?
              case @subtype
                when :integer
                  value.to_i
                when :datetime
                  DateTime.new.serialize(value)
                when :date
                  Date.new.serialize(value)
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
