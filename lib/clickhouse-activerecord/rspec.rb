# frozen_string_literal: true

RSpec.configure do |config|
  config.before do
    original_connection_config = ActiveRecord::Base.connection_db_config
    ActiveRecord::Base.configurations.configurations.select { |x| x.env_name == Rails.env && x.adapter == 'clickhouse' }.each do |db_config|
      ActiveRecord::Base.establish_connection(db_config)
      ActiveRecord::Base.connection.truncate_tables(*ActiveRecord::Base.connection.tables)
    end
    ActiveRecord::Base.establish_connection(original_connection_config)
  end
end
