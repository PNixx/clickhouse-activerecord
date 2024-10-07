# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class Map < Type::Value # :nodoc:

          def initialize(sql_type)
            case sql_type
            when /U?Int(\d+)/
              @subtype = :integer
              @limit = bits_to_limit(Regexp.last_match(1)&.to_i)
            when /DateTime/
              @subtype = :datetime
            when /Date/
              @subtype = :date
            else
              @subtype = :string
            end
          end

          def type
            @subtype
          end

          def deserialize(value)
            if value.is_a?(::Hash)
              value.map { |k, item| [k.to_s, deserialize(item)] }.to_h
            elsif value.is_a?(::Array)
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
              else
                super
              end
            end
          end

          def serialize(value)
            if value.is_a?(::Hash)
              value.map { |k, item| [k.to_s, serialize(item)] }.to_h
            elsif value.is_a?(::Array)
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
                when :string
                  value.to_s
              else
                super
              end
            end
          end

          private

          def bits_to_limit(bits)
            case bits
            when 8   then 1
            when 16  then 2
            when 32  then 4
            when 64  then 8
            when 128 then 16
            when 256 then 32
            end
          end

        end
      end
    end
  end
end
