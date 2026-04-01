# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, id: false, options: 'MergeTree ORDER BY an_id', ttl: 'date + INTERVAL 30 DAY', settings: 'allow_nullable_key = 1, index_granularity = 8192' do |t|
      t.uuid :an_id, null: true
      t.date :date, null: false
      t.integer :data, null: false
    end
  end
end
