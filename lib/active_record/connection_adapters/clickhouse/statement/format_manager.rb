# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Statement
        class FormatManager

          def initialize(sql, format:)
            @sql = sql.strip
            @format = format
          end

          def apply
            return @sql if skip_format? || @format.blank?

            "#{@sql} FORMAT #{@format}"
          end

          def skip_format?
            system_command? || schema_command? || format_specified? || delete?
          end

          private

          def system_command?
            /\Asystem|\Aoptimize/i.match?(@sql)
          end

          def schema_command?
            /\Acreate|\Aalter|\Adrop|\Arename/i.match?(@sql)
          end

          def format_specified?
            /format [a-z]+\z/i.match?(@sql)
          end

          def delete?
            /\Adelete from/i.match?(@sql)
          end

        end
      end
    end
  end
end
