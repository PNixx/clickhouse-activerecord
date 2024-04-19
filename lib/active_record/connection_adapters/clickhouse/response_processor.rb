# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class ResponseProcessor

        def initialize(raw_response)
          @raw_response = raw_response
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

          JSON.parse(@raw_response.body) if @raw_response.body.present?
        end

        def raise_generic!
          raise ActiveRecord::ActiveRecordError, "Response code: #{@raw_response.code}:\n#{@raw_response.body}"
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
