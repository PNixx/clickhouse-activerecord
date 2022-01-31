# frozen_string_literal: true

class ModifySomeTable < ActiveRecord::Migration[5.0]
  def up
    add_column :some, :new_column, :big_integer
  end
end
