# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[5.0]
  def up
    create_table :some do |t|
      t.decimal :money, precision: 16, scale: 4
    end
  end
end

