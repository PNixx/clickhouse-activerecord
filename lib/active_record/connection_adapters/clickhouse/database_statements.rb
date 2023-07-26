# frozen_string_literal: true

require 'clickhouse-activerecord/version'

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module DatabaseStatements
        def with_settings(**settings)
          @block_settings ||= {}
          prev_settings = @block_settings
          @block_settings.merge! settings
          yield
        ensure
          @block_settings = prev_settings
        end

        def execute(sql, name = nil, settings: {})
          log(sql, "#{adapter_name} #{name}") do
            formatted_sql = apply_format(sql)
            res = @connection.post("/?#{settings_params(settings)}", formatted_sql, 'User-Agent' => "Clickhouse ActiveRecord #{ClickhouseActiverecord::VERSION}")

            process_response(res)
          end
        end

        # Executes insert +sql+ statement in the context of this connection using
        # +binds+ as the bind substitutes. +name+ is logged along with
        # the executed +sql+ statement.
        def exec_insert(sql, name, _binds, _pk = nil, _sequence_name = nil)
          new_sql = sql.sub(/ (DEFAULT )?VALUES/, " VALUES")
          execute(new_sql, name)
          true
        end

        def exec_query(sql, name = nil, binds = [], prepare: false)
          result = execute(sql, name)
          ActiveRecord::Result.new(result['meta'].map { |m| m['name'] }, result['data'])
        rescue ActiveRecord::ActiveRecordError => e
          raise e
        rescue StandardError => e
          raise ActiveRecord::ActiveRecordError, "Response: #{e.message}"
        end

        def exec_insert_all(sql, name)
          execute(sql, name)
          true
        end

        def exec_update(_sql, _name = nil, _binds = [])
          raise ActiveRecord::ActiveRecordError, 'Clickhouse update is not supported'
        end

        def exec_delete(_sql, _name = nil, _binds = [])
          raise ActiveRecord::ActiveRecordError, 'Clickhouse delete is not supported'
        end

        def do_system_execute(sql, name = nil)
          log_with_debug(sql, "#{adapter_name} #{name}") do
            res = @connection.post("/?#{@config.to_param}", "#{sql} FORMAT JSONCompact", 'User-Agent' => "Clickhouse ActiveRecord #{ClickhouseActiverecord::VERSION}")

            process_response(res)
          end
        end

        protected

        def last_inserted_id(result)
          result
        end

        private

        def apply_cluster(sql)
          return sql unless cluster

          normalized_cluster_name = cluster.start_with?('{') ? "'#{cluster}'" : cluster
          "#{sql} ON CLUSTER #{normalized_cluster_name}"
        end

        def apply_format(sql)
          return sql unless formattable?(sql)

          "#{sql} FORMAT #{ClickhouseAdapter::DEFAULT_FORMAT}"
        end

        def formattable?(sql)
          !for_insert?(sql) && !system_command?(sql)
        end

        def for_insert?(sql)
          /^insert into/i.match?(sql)
        end

        def system_command?(sql)
          /^system|optimize/i.match?(sql)
        end

        def process_response(res)
          raise ActiveRecord::ActiveRecordError, "Response code: #{res.code}:\n#{res.body}" unless res.code.to_i == 200

          JSON.parse(res.body) if res.body.present?
        rescue JSON::ParserError
          res.body
        end

        def log_with_debug(sql, name = nil)
          return yield unless @debug

          log(sql, "#{name} (system)") { yield }
        end

        def settings_params(settings = {})
          request_params = @config || {}
          block_settings = @block_settings || {}
          request_params.merge(block_settings)
                        .merge(settings)
                        .to_param
        end
      end
    end
  end
end
