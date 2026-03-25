# frozen_string_literal: true

class AddColumnsWithLimit < ActiveRecord::Migration[7.1]
  def up
    # Signed integers with limit
    add_column :some, :int16_col, :integer, limit: 2, unsigned: false, null: false, default: 0
    add_column :some, :int32_col, :integer, limit: 4, unsigned: false, null: false, default: 0
    add_column :some, :int64_limit5_col, :integer, limit: 5, unsigned: false, null: false, default: 0
    add_column :some, :int64_col, :integer, limit: 8, unsigned: false, null: false, default: 0

    # Unsigned integers with limit
    add_column :some, :uint8_col, :integer, limit: 1, null: false, default: 0
    add_column :some, :uint16_col, :integer, limit: 2, null: false, default: 0
    add_column :some, :uint64_col, :integer, limit: 8, null: false, default: 0

    # Default unsigned integer (no limit)
    add_column :some, :uint32_col, :integer, null: false, default: 0
  end
end
