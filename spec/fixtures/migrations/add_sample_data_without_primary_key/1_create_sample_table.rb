# frozen_string_literal: true

class CreateSampleTable < ActiveRecord::Migration[7.1]
  def up
    create_table :sample_without_key, id: false, options: 'Log' do |t|
      t.string :event_name, null: false
      t.integer :event_value
      t.boolean :enabled, null: false, default: false
      t.date :date, null: false
      t.datetime :datetime, null: false
      t.datetime :datetime64, precision: 3
      t.string :byte_array
      t.uuid :relation_uuid
      t.decimal :decimal_value, precision: 38, scale: 16
    end
  end
end
