# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, options: 'MergeTree ORDER BY id' do |t|
      t.integer :col, limit: 4, null: false
    end
  end
end

