# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[5.0]
  def up
    create_table :some do |t|
      t.datetime :datetime, null: false
      t.datetime :datetime64, precision: 3, null: true
    end
  end
end

