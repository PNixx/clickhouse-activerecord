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

        private

        def deduplicated
          self
        end
      end
    end
  end
end
