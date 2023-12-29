# frozen_string_literal: true

class CreateEventsTable < ActiveRecord::Migration[5.0]
  def up
    opts = <<~SQL.squish
      MergeTree
      PARTITION BY date
      ORDER BY (date, event_name)
      SETTINGS index_granularity = 8192
    SQL
    create_table :events, options: opts do |t|
      t.string :event_name, null: false
      t.integer :event_value, null: false
      t.date :date, null: false
    end
  end
end
