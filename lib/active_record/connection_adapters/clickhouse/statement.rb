# frozen_string_literal: true

require 'active_record/connection_adapters/clickhouse/statement/format_manager'
require 'active_record/connection_adapters/clickhouse/statement/response_processor'

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Statement

        attr_reader :format
        attr_writer :response

        def initialize(sql, format:)
          @sql = sql
          @format = format
        end

        def formatted_sql
          @formatted_sql ||= FormatManager.new(@sql, format: @format).apply
        end

        def processed_response
          ResponseProcessor.new(@response, @format, @sql).process
        end

      end
    end
  end
end
