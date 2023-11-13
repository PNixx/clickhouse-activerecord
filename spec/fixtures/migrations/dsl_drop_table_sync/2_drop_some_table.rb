# frozen_string_literal: true

class DropSomeTable < ActiveRecord::Migration[5.0]
  def up
    drop_table :some, sync: true
  end
end

