# frozen_string_literal: true

module ClickhouseActiverecord
  module TestHelper
    def before_setup
      super
      ActiveRecord::Base.configurations.configurations.select { |x| x.env_name == Rails.env && x.adapter == 'clickhouse' }.each do |config|
        ActiveRecord::Base.establish_connection(config)
        ActiveRecord::Base.connection.truncate_tables(*ActiveRecord::Base.connection.tables)
      end
    end
  end
end

ActiveSupport::TestCase.include(ClickhouseActiverecord::TestHelper) if defined?(ActiveSupport::TestCase)
