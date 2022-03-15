# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[5.0]
  def up
    create_table :some do |t|
      t.integer :col8, null: false, limit: 1
      t.integer :col16, null: false, limit: 2
      t.integer :col32, null: false
      t.integer :col64, null: false, limit: 8
      t.integer :col128, null: false, limit: 16
      t.integer :col256, null: false, limit: 32
    end
  end
end

