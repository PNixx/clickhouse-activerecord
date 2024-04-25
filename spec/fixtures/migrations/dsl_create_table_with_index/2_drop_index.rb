# frozen_string_literal: true

class DropIndex < ActiveRecord::Migration[7.1]
  def up
    remove_index :some, 'idx'
  end
end

