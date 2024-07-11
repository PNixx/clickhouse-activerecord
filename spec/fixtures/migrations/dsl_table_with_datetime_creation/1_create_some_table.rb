# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, id: false, force: true do |t|
      t.datetime :datetime, null: false
      t.datetime :datetime64, precision: 3, null: true
    end
  end
end

