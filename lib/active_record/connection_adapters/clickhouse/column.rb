module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Column < ActiveRecord::ConnectionAdapters::Column

        attr_reader :codec

        def initialize(*, codec: nil, **)
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
