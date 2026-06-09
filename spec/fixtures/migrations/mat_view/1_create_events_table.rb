# frozen_string_literal: true

class CreateEventsTable < ActiveRecord::Migration[5.0]
  def up
    create_table :events, options: 'MergeTree ORDER BY (date, event_name)' do |t|
      t.string :event_name, null: false
      t.date :date, null: false
    end
  end
end
