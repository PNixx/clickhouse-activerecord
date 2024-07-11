# frozen_string_literal: true

class CreateActionsTable < ActiveRecord::Migration[7.1]
  def up
    create_table :actions, options: 'MergeTree ORDER BY date', force: true do |t|
      t.datetime :array_datetime, null: false, array: true
      t.string :array_string, null: false, array: true
      t.integer :array_int, null: false, array: true
      t.date :date, null: false
    end
  end
end

