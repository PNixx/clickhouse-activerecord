# frozen_string_literal: true

class AlterColColumn < ActiveRecord::Migration[7.1]
  def up
    change_column :some, :col, :int64, null: false
  end
end
