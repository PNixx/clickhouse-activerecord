module Clickhouse
  require 'rails'

  class Railtie < Rails::Railtie
    rake_tasks { load 'tasks/clickhouse.rake' }
  end
end
