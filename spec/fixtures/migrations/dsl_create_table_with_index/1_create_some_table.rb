# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, options: 'MergeTree PARTITION BY toYYYYMM(date) ORDER BY (date)' do |t|
      t.integer :int1, null: false
      t.integer :int2, null: false
      t.date :date, null: false

      t.index '(int1 * int2, date)', name: 'idx', type: 'minmax', granularity: 3
    end
  end
end

