# frozen_string_literal: true

class CreateSecondaryJoinTable < ActiveRecord::Migration[5.0]
  def up
    create_table :secondary_joins, options: 'MergeTree PARTITION BY toYYYYMM(date) ORDER BY (event_name)' do |t|
      t.string :event_name, null: false
      t.integer :join_value
      t.date :date, null: false
    end
  end
end
