# frozen_string_literal: true

class ModifySomeTable < ActiveRecord::Migration[7.1]
  def up
    add_column :some, :new_column, :big_integer
  end
end

