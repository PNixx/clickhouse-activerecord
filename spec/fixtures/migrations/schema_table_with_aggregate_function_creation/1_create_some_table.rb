# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, id: false do |t|
      t.column :col1, "AggregateFunction(sum, Float32)", null: false
      t.column :col2, "AggregateFunction(anyLast, Float64)", null: false
      t.column :col3, "AggregateFunction(anyLast, DateTime64)", null: false
    end
  end
end
