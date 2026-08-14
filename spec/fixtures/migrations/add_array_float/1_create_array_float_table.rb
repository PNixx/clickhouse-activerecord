# frozen_string_literal: true

class CreateArrayFloatTable < ActiveRecord::Migration[7.1]
  def up
    create_table :array_float_test, options: 'MergeTree ORDER BY date', force: true do |t|
      t.column :array_float32, 'Array(Float32)', null: false
      t.column :array_float64, 'Array(Float64)', null: false
      t.date :date, null: false
    end
  end
end
