# frozen_string_literal: true

require 'active_record/connection_adapters/clickhouse/statement/format_manager'
require 'active_record/connection_adapters/clickhouse/statement/response_processor'

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Statement

        attr_reader :format

        def initialize(sql, format:)
          @sql = sql
          @format = format
        end

        # @return [String]
        def formatted_sql
          @formatted_sql ||= FormatManager.new(@sql, format: @format).apply
        end

        # @param [Net::HTTPResponse] response
        # @return [String, Hash, Array, nil]
        def processed_response(response)
          ResponseProcessor.new(response, @format, @sql).process
        end

        # @param [Net::HTTPResponse] response
        # @return [String, nil]
        def streaming_response(response)
          ResponseProcessor.new(response, @format, @sql).streaming_process
        end

      end
    end
  end
end
