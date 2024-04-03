# frozen_string_literal: true

class CreateSampleTable < ActiveRecord::Migration[5.0]
  def up
    create_table :sample, options: 'ReplacingMergeTree PARTITION BY toYYYYMM(date) ORDER BY (event_name)' do |t|
      t.string :event_name, null: false
      t.integer :event_value
      t.boolean :enabled, null: false, default: false
      t.date :date, null: false
      t.datetime :datetime, null: false
      t.datetime :datetime64, precision: 3, null: true
      t.string :byte_array, null: true
      t.uuid :relation_uuid
      t.decimal :decimal_value, precision: 38, scale: 16, null: true
    end
  end
end
