module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Column < ActiveRecord::ConnectionAdapters::Column

        attr_reader :codec

        def initialize(name, default, sql_type_metadata = nil, null = true, default_function = nil, codec: nil, **args)
          super
          @codec = codec
        end

        private

        def deduplicated
          self
        end
      end
    end
  end
end
