# frozen_string_literal: true

class ModifySomeTable < ActiveRecord::Migration[7.1]
  def up
    add_column :some, :new_column, :big_integer
    add_column :some, :new_uint16, :integer, limit: 2, unsigned: true
  end
end
