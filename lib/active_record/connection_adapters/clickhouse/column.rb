module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      DescribedColumn =
        Data.define(:name, :sql_type, :default_type, :default_expression, :comment, :codec) do
          def ephemeral?
            default_type.to_s.downcase == 'ephemeral'
          end
        end

      class Column < ActiveRecord::ConnectionAdapters::Column
        attr_reader :codec, :default_kind

        def initialize(*, codec: nil, default_kind: nil, **)
          super
          @codec = codec
          @default_kind = ActiveSupport::StringInquirer.new(default_kind.to_s.downcase.presence || 'none')
        end

        def virtual?
          default_kind.materialized? || default_kind.alias?
        end

        # Whether the column holds a ClickHouse `Array(...)` type.
        #
        # ActiveRecord core never calls this, but tooling such as annotaterb
        # detects array columns via `column.respond_to?(:array) && column.array`,
        # so the base adapter's missing `array` reader left them undetected.
        def array
          sql_type.start_with?('Array(')
        end

        private

        def deduplicated
          self
        end
      end
    end
  end
end
