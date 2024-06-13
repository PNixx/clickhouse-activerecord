# frozen_string_literal: true

class CreateIndex < ActiveRecord::Migration[7.1]
  def up
    add_index :some, 'int1 * int2', name: 'idx2', type: 'set(10)', granularity: 4
  end
end

