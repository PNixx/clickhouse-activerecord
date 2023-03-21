# frozen_string_literal: true

module ClickhouseActiverecord
  require 'rails'

  class Railtie < Rails::Railtie
    initializer "clickhouse.load" do
      ActiveSupport.on_load :active_record do
        ClickhouseActiverecord.load
      end
    end

    rake_tasks { load 'tasks/clickhouse.rake' }
  end
end
