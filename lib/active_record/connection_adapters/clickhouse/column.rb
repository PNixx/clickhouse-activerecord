module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Column < ActiveRecord::ConnectionAdapters::Column

        attr_reader :codec, :ttl

        def initialize(*, codec: nil, ttl: nil, **)
          super
          @codec = codec
          @ttl = ttl
        end

        private

        def deduplicated
          self
        end
      end
    end
  end
end
