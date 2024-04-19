# frozen_string_literal: true

require 'active_record/connection_adapters/clickhouse/response_processor'
require 'active_record/connection_adapters/clickhouse/sql_formatter'

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Statement

        attr_reader :format
        attr_writer :response

        def initialize(sql, format: nil)
          @sql = sql
          @format = format || ClickhouseAdapter::DEFAULT_FORMAT
        end

        def formatted_sql
          @formatted_sql ||= SqlFormatter.new(@sql, format: @format).apply
        end

        def processed_response
          ResponseProcessor.new(@response).process
        end

      end
    end
  end
end
