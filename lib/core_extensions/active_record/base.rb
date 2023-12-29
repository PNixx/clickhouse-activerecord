# frozen_string_literal: true

module CoreExtensions
  module ActiveRecord
    module Base
      module ClassMethods
        delegate :final, :final!, :settings, :settings!, to: :all

        # Establishes a connection to the database that's used by all Active Record objects
        def clickhouse_connection(config)
          config = config.symbolize_keys

          if config[:connection]
            connection = {
              connection: config[:connection]
            }
          else
            port       = config[:port] || 8123
            connection = {
              host:          config[:host] || 'localhost',
              port:          port,
              ssl:           config[:ssl].present? ? config[:ssl] : port == 443,
              sslca:         config[:sslca],
              read_timeout:  config[:read_timeout],
              write_timeout: config[:write_timeout],
            }
          end

          if config.key?(:database)
            database = config[:database]
          else
            raise ArgumentError, 'No database specified. Missing argument: database.'
          end

          ::ActiveRecord::ConnectionAdapters::ClickhouseAdapter.new(logger, connection, { user: config[:username], password: config[:password], database: database }.compact, config)
        end

        def is_view
          return false unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

          @is_view || false
        end

        # @param [Boolean] value
        def is_view=(value)
          raise NotImplementedError, "Only used by models backed by Clickhouse" unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

          @is_view = value
        end
      end
    end
  end
end
