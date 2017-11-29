require 'active_record/connection_adapters/clickhouse_adapter'

if defined?(Rails::Railtie)
  require 'clickhouse-activerecord/railtie'
  require 'clickhouse-activerecord/schema_dumper'
end

module ClickhouseActiverecord

end
