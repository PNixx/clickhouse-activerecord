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

        end
      end
    end
  end
end
