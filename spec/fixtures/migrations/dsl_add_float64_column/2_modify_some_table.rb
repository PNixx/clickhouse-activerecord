# frozen_string_literal: true

class ModifySomeTable < ActiveRecord::Migration[7.1]
  def up
    add_column :some, :float32, :float, null: false, default: 0
    add_column :some, :float64, :float, limit: 8, null: false, default: 0
  end
end
