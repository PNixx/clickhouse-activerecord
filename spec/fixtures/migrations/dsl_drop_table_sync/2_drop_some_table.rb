# frozen_string_literal: true

class DropSomeTable < ActiveRecord::Migration[7.1]
  def up
    drop_table :some, sync: true
  end
end

