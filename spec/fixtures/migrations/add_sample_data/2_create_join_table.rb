# frozen_string_literal: true

class CreateJoinTable < ActiveRecord::Migration[7.1]
  def up
    create_table :joins, options: 'MergeTree PARTITION BY toYYYYMM(date) ORDER BY (event_name)' do |t|
      t.string :event_name, null: false
      t.integer :event_value
      t.integer :join_value
      t.date :date, null: false
    end
  end
end

