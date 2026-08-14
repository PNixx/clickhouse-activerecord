# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class Array < Type::Value # :nodoc:
          JSON_ARRAY_TYPE = /Array\((?:Nullable\()?JSON(?:\))?\)/i

          def initialize(sql_type)
            @json_type = ActiveRecord::Type::Json.new if sql_type.match?(JSON_ARRAY_TYPE)

            @subtype = if @json_type
                         :json
                       else
                         case sql_type
                         when /U?Int\d+/, /U?Float\d+/
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
              when :datetime
                ::DateTime.parse(value)
              when :date
                ::Date.parse(value)
              when :json
                @json_type.deserialize(value)
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
              when :datetime
                DateTime.new.serialize(value)
              when :date
                Date.new.serialize(value)
              when :json
                @json_type.serialize(value)
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
