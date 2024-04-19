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

        def with_response_format(format)
          prev_format = @response_format
          @response_format = format
          yield
        ensure
          @response_format = prev_format
        end

        def execute(sql, name = nil, settings: {})
          log(sql, [adapter_name, name].compact.join(' ')) do
            raw_execute(sql, settings: settings)
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

        def do_system_execute(sql, name = nil, except_params: [])
          log_with_debug(sql, [adapter_name, name].compact.join(' ')) do
            raw_execute(sql, except_params: except_params)
          end
        end

        # Fix insert_all method
        # https://github.com/PNixx/clickhouse-activerecord/issues/71#issuecomment-1923244983
        def with_yaml_fallback(value) # :nodoc:
          return value if value.is_a?(Array)

          super
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

        def raw_execute(sql, settings: {}, except_params: [])
          statement = Statement.new(sql)
          statement.response = post_statement(statement, settings: settings, except_params: except_params)
          statement.processed_response
        end

        def post_statement(statement, settings: {}, except_params: [])
          @connection.post("/?#{settings_params(settings, except: except_params)}",
                           statement.formatted_sql,
                           'User-Agent' => ClickhouseAdapter::USER_AGENT)
        end

        def log_with_debug(sql, name = nil)
          return yield unless @debug

          log(sql, "#{name} (system)") { yield }
        end

        def settings_params(settings = {}, except: [])
          request_params = @connection_config || {}
          block_settings = @block_settings || {}
          request_params.merge(block_settings)
                        .merge(settings)
                        .except(*except)
                        .to_param
        end
      end
    end
  end
end
