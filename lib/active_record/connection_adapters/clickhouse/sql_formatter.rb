# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class SqlFormatter
        def initialize(sql, format:)
          @sql = sql
          @format = format
        end

        def apply
          return @sql if skip_format?

          "#{@sql} FORMAT #{@format}"
        end

        def skip_format?
          for_insert? || system_command? || schema_command? || format_specified? || delete?
        end

        private

        def for_insert?
          /^insert into/i.match?(@sql)
        end

        def system_command?
          /^system|^optimize/i.match?(@sql)
        end

        def schema_command?
          /^create|^alter|^drop|^rename/i.match?(@sql)
        end

        def format_specified?
          /format [a-z]+\z/i.match?(@sql)
        end

        def delete?
          /^delete from/i.match?(@sql)
        end

      end
    end
  end
end
