class CreateVerbsTable < ActiveRecord::Migration[7.1]
  def up
    create_table :verbs, options: 'MergeTree ORDER BY date', force: true do |t|
      t.datetime :map_datetime, null: false, map: true
      t.string :map_string, null: false, map: true
      t.integer :map_int, null: false, map: true

      t.datetime :map_array_datetime, null: false, map: :array
      t.string :map_array_string, null: false, map: :array
      t.integer :map_array_int, null: false, map: :array

      t.date :date, null: false
    end
  end
end
