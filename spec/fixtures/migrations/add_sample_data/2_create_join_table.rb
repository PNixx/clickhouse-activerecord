# frozen_string_literal: true

class CreateJoinTable < ActiveRecord::Migration[5.0]
  def up
    opts = <<~SQL.squish
      MergeTree
      PARTITION BY toYYYYMM(date)
      ORDER BY (event_name)
    SQL
    create_table :joins, options: opts do |t|
      t.string :event_name, null: false
      t.integer :event_value
      t.integer :join_value
      t.date :date, null: false
    end
  end
end

