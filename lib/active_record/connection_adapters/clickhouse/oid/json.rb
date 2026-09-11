# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module OID # :nodoc:
        class Json < Type::Json # :nodoc:

          def deserialize(value)
            if value.is_a?(::String)
              ::JSON.parse(value)
            else
              super
            end
          rescue ::JSON::ParserError => e
            ActiveSupport.error_reporter.report(e, source: "application.active_record")
            nil
          end
        end
      end
    end
  end
end
