# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, id: false do |t|
      t.string :col1, low_cardinality: true, null: false
      t.string :col2, low_cardinality: true, null: true
      t.string :col3, low_cardinality: true, array: true, null: true
    end
  end
end
