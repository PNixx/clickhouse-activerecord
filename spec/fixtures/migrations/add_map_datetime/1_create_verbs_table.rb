class CreateVerbsTable < ActiveRecord::Migration[7.1]
  def up
    create_table :verbs, options: 'MergeTree ORDER BY date', force: true do |t|
      t.datetime :map_datetime, null: false, map: true
      t.string :map_string, null: false, map: true
      t.integer :map_int, null: false, map: true
      t.date :date, null: false
    end
  end
end

