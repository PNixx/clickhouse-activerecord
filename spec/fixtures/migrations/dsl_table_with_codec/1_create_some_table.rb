# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, id: false, force: true do |t|
      t.column :custom, "Nullable(UInt64) CODEC(T64, LZ4)"
    end
  end
end

