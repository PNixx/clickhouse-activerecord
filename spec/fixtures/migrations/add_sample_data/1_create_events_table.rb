# frozen_string_literal: true

class CreateEventsTable < ActiveRecord::Migration[5.0]
  def up
    create_table :events, options: 'MergeTree PARTITION BY toYYYYMM(date) ORDER BY (event_name)' do |t|
      t.string :event_name, null: false
      t.integer :event_value
      t.boolean :enabled, null: false, default: false
      t.date :date, null: false
    end
  end
end

