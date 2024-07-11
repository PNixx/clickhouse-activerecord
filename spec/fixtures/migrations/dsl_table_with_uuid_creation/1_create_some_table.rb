# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some, id: false do |t|
      t.uuid :col1, null: false
      t.uuid :col2, null: true
    end
  end
end

