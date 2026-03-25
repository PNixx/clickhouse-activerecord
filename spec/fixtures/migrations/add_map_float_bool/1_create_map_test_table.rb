# frozen_string_literal: true

class CreateMapTestTable < ActiveRecord::Migration[7.1]
  def up
    create_table :map_test, options: 'MergeTree ORDER BY date', force: true do |t|
      t.column :map_float32, 'Map(String, Float32)', null: false
      t.column :map_float64, 'Map(String, Float64)', null: false
      t.column :map_bool, 'Map(String, Bool)', null: false
      t.date :date, null: false
    end
  end
end
