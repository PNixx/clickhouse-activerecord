require 'spec_helper'

RSpec.describe 'Materialized Views' do
  before do
    ActiveRecord::Schema.define do
      create_table "events", id: false, options: "Log", force: :cascade do |t|
        t.integer "quantity", default: -> { "CAST(1, 'Int8')" }, null: false
        t.string "name", null: false
        t.date "created_at", null: false
      end
    end
  end

  after do
    ActiveRecord::Schema.define do
      drop_table :events if table_exists?(:events)
      drop_table :aggregated_events_mv if table_exists?(:aggregated_events_mv)
      drop_table :aggregated_events if table_exists?(:aggregated_events)
    end
  end

  it 'creates a materialized view with TO clause and column definitions' do
    database = ActiveRecord::Base.connection_db_config.database

    ActiveRecord::Schema.define do
      create_table "aggregated_events", id: false, options: "SummingMergeTree ORDER BY (name, date) SETTINGS index_granularity = 8192", force: :cascade do |t|
        t.string "name", null: false
        t.date "date", null: false
        t.integer "total_quantity", limit: 8, null: false
        t.integer "event_count", limit: 8, null: false
      end

      create_table "aggregated_events_mv", view: true, materialized: true, to: "#{database}.aggregated_events", id: false, as: "SELECT name, created_at AS date, sum(quantity) AS total_quantity, count() AS event_count FROM #{database}.events GROUP BY name, created_at", force: :cascade do |t|
      end
    end

    # Verify the view was created correctly
    result = ActiveRecord::Base.connection.do_system_execute(
      "SHOW CREATE TABLE #{database}.aggregated_events_mv"
    )['data'].first.first

    expect(result.squish).to eq('CREATE MATERIALIZED VIEW default.aggregated_events_mv TO default.aggregated_events ( `name` String, `date` Date, `total_quantity` UInt64, `event_count` UInt64 ) AS SELECT name, created_at AS date, sum(quantity) AS total_quantity, count() AS event_count FROM default.events GROUP BY name, created_at')
  end
end
