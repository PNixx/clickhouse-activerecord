# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, id: false, options: 'MergeTree ORDER BY date', ttl: 'date + INTERVAL 30 DAY' do |t|
      t.date :date, null: false
      t.integer :data, null: false, ttl: 'date + INTERVAL 1 DAY'
    end
  end
end
