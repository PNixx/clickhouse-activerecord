# frozen_string_literal: true

RSpec.configure do |config|
  config.before do
    ActiveRecord::Base.configurations.configurations.select { |x| x.env_name == Rails.env && x.adapter == 'clickhouse' }.each do |config|
      ActiveRecord::Base.establish_connection(config)
      ActiveRecord::Base.connection.tables.each do |table|
        ActiveRecord::Base.connection.execute("TRUNCATE TABLE #{table}")
      end
    end
  end
end
