# frozen_string_literal: true

module ClickhouseActiverecord
  module TestHelper
    def before_setup
      super
      original_connection_config = ActiveRecord::Base.connection_db_config
      ActiveRecord::Base.configurations.configurations.select { |x| x.env_name == Rails.env && x.adapter == 'clickhouse' }.each do |config|
        ActiveRecord::Base.establish_connection(config)
        ActiveRecord::Base.connection.truncate_tables(*ActiveRecord::Base.connection.tables)
      end
    ensure
      ActiveRecord::Base.establish_connection(original_connection_config) if original_connection_config
    end
  end
end

ActiveSupport::TestCase.include(ClickhouseActiverecord::TestHelper) if defined?(ActiveSupport::TestCase)
