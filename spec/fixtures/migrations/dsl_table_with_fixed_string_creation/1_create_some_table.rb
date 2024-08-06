# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, id: false do |t|
      t.string :fixed_string1, fixed_string: 1, null: false
      t.string :fixed_string16_array, fixed_string: 16, array: true, null: true
      t.string :fixed_string16_map, fixed_string: 16, map: true, null: true
    end
  end
end
