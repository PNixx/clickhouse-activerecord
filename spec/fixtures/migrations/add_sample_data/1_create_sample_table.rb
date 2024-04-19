# frozen_string_literal: true

class CreateSampleTable < ActiveRecord::Migration[5.0]
  def up
    opts = <<~SQL.squish
      ReplacingMergeTree
      PARTITION BY date
      ORDER BY (date, event_name)
      SETTINGS index_granularity = 8192
    SQL
    create_table :sample, options: opts do |t|
      t.string :event_name, null: false
      t.integer :event_value, null: false
      t.boolean :enabled, null: false, default: false
      t.date :date, null: false
      t.datetime :datetime, null: false
      t.datetime :datetime64, precision: 3, null: true
      t.string :byte_array, null: true
      t.uuid :relation_uuid
    end
  end
end
