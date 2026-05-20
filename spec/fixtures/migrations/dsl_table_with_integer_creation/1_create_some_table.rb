# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, id: false do |t|
      t.integer :default_int
      t.integer :signed_no_limit, unsigned: false, null: false, default: -1
      t.integer :signed_small, unsigned: false, limit: 1, null: false, default: -1
      t.integer :signed_int32, unsigned: false, limit: 4, null: false, default: -1
      t.integer :signed_int64, unsigned: false, limit: 8, null: false, default: -1
      t.integer :unsigned_tiny, unsigned: true, limit: 1, null: false, default: 0
    end
  end
end
