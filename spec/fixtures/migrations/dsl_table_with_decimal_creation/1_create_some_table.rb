# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some do |t|
      t.decimal :money, precision: 16, scale: 4
      t.decimal :balance, precision: 32, scale: 2, null: false, default: 0
      t.decimal :paid, precision: 32, scale: 2, null: false, default: 1.15
    end
  end
end

