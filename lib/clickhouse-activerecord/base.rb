# frozen_string_literal: true

module ClickhouseActiverecord
  module Base
    module ClassMethods
      # Establishes a connection to the database that's used by all Active Record objects
      def clickhouse_connection(config)
        config = config.symbolize_keys

        if config[:connection]
          connection = {
            connection: config[:connection]
          }
        else
          port = config[:port] || 8123
          connection = {
            host: config[:host] || 'localhost',
            port: port,
            ssl: config[:ssl].present? ? config[:ssl] : port == 443,
            sslca: config[:sslca],
            read_timeout: config[:read_timeout],
            write_timeout: config[:write_timeout],
          }
        end

        if config.key?(:database)
          database = config[:database]
        else
          raise ArgumentError, 'No database specified. Missing argument: database.'
        end

        ActiveRecord::ConnectionAdapters::ClickhouseAdapter.new(logger, connection, { user: config[:username], password: config[:password], database: database }.compact, config)
      end

      def is_view
        @is_view || false
      end

      # @param [Boolean] value
      def is_view=(value)
        @is_view = value
      end
    end
  end
end
