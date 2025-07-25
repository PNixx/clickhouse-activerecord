# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Statement
        class ResponseProcessor

          def initialize(raw_response, format)
            @raw_response = raw_response
            @format = format
          end

          def process
            if success?
              process_successful_response
            else
              raise_database_error!
            end
          rescue JSON::ParserError
            @raw_response.body
          end

          private

          def success?
            @raw_response.code.to_i == 200
          end

          def process_successful_response
            raise_generic! if @raw_response.body.to_s.include?('DB::Exception')

            format_body_response
          end

          def raise_generic!
            raise ActiveRecord::ActiveRecordError, "Response code: #{@raw_response.code}:\n#{@raw_response.body}"
          end

          def format_body_response
            body = @raw_response.body
            return body if body.blank?

            case @format
            when 'JSONCompact'
              format_from_json_compact(body)
            when 'JSONCompactEachRowWithNamesAndTypes'
              format_from_json_compact_each_row_with_names_and_types(body)
            else
              body
            end
          rescue JSON::ParserError
            @raw_response.body
          end

          def format_from_json_compact(body)
            parse_json_payload(body)
          end

          def format_from_json_compact_each_row_with_names_and_types(body)
            rows = body.split("\n").map { |row| parse_json_payload(row) }
            names, types, *data = rows

            meta = names.zip(types).map do |name, type|
              {
                'name' => name,
                'type' => type
              }
            end

            {
              'meta' => meta,
              'data' => data
            }
          end

          def parse_json_payload(payload)
            JSON.parse(payload, decimal_class: BigDecimal)
          end

          def raise_database_error!
            case @raw_response.body
            when /DB::Exception:.*\(UNKNOWN_DATABASE\)/
              raise ActiveRecord::NoDatabaseError
            when /DB::Exception:.*\(DATABASE_ALREADY_EXISTS\)/
              raise ActiveRecord::DatabaseAlreadyExists
            else
              raise_generic!
            end
          end

        end
      end
    end
  end
end
