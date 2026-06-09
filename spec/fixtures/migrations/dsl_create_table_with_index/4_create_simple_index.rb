# frozen_string_literal: true

class CreateSimpleIndex < ActiveRecord::Migration[7.1]
  def up
    add_index :some, 'date', name: 'simple_idx', type: 'minmax', granularity: 1
  end
end
