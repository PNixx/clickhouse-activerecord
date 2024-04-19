# frozen_string_literal: true

require 'active_record/connection_adapters/clickhouse/response_processor'
require 'active_record/connection_adapters/clickhouse/sql_formatter'
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
        def exec_insert(sql, name, _binds, _pk = nil, _sequence_name = nil, returning: nil)
          new_sql = sql.sub(/ (DEFAULT )?VALUES/, " VALUES")
          execute(new_sql, name)
          true
        end

        exec_method_name =
          if ActiveRecord.version < Gem::Version.new('7.1')
            :exec_query
          else
            :internal_exec_query
          end

        define_method exec_method_name do |sql, name = nil, _binds = [], prepare: false, async: false|
          result = execute(sql, name)
          ActiveRecord::Result.new(result['meta'].map { |m| m['name'] }, result['data'], result['meta'].map { |m| [m['name'], type_map.lookup(m['type'])] }.to_h)
        rescue ActiveRecord::ActiveRecordError => e
          raise e
        rescue StandardError => e
          raise ActiveRecord::ActiveRecordError, "Response: #{e.message}"
        end

        def exec_insert_all(sql, name)
          execute(sql, name)
          true
        end

        # @link https://clickhouse.com/docs/en/sql-reference/statements/alter/update
        def exec_update(sql, name = nil, _binds = [])
          raise ActiveRecord::ActiveRecordError, 'ClickHouse update is not supported' unless supports_update?

          execute(sql, name)
          true
        end

        # @link https://clickhouse.com/docs/en/sql-reference/statements/delete
        def exec_delete(sql, name = nil, _binds = [])
          raise ActiveRecord::ActiveRecordError, 'ClickHouse delete is not supported' unless supports_delete?

          execute(sql, name)
          true
        end

        def do_system_execute(sql, name = nil)
          log_with_debug(sql, "#{adapter_name} #{name}") do
            res = @connection.post("/?#{@connection_config.to_param}", "#{sql} FORMAT JSONCompact", 'User-Agent' => "Clickhouse ActiveRecord #{ClickhouseActiverecord::VERSION}")

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
          SqlFormatter.new(sql).apply
        end

        def process_response(res)
          ResponseProcessor.new(res).process
        end

        def log_with_debug(sql, name = nil)
          return yield unless @debug

          log(sql, "#{name} (system)") { yield }
        end

        def settings_params(settings = {})
          request_params = @connection_config || {}
          block_settings = @block_settings || {}
          request_params.merge(block_settings)
                        .merge(settings)
                        .to_param
        end
      end
    end
  end
end
