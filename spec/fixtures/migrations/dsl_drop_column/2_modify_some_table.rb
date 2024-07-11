# frozen_string_literal: true

class ModifySomeTable < ActiveRecord::Migration[7.1]
  def up
    remove_column :some, :id
  end
end

