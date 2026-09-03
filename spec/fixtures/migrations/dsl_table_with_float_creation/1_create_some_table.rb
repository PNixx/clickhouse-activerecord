# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, id: false do |t|
      t.float :default_float
      t.float :float32, limit: 4, null: false, default: 0
      t.float :float64, limit: 8, null: false, default: 0
      t.float :nullable_float64, limit: 8
    end
  end
end
