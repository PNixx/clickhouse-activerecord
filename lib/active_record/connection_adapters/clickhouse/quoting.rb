# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module Quoting
        def unquote_string(s)
          s.gsub(/\\(.)/, '\1')
        end
      end
    end
  end
end
