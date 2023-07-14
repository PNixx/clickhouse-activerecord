# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module Quoting
        def quote(value)
          case value
          when Array
            '[' + value.map { |v| quote(v) }.join(', ') + ']'
          else
            super
          end
        end

        def unquote_string(s)
          s.gsub(/\\(.)/, '\1')
        end

        def sanitize_as_setting_name(value) # :nodoc:
          value.to_s.gsub(/\W+/, "")
        end
      end
    end
  end
end
