# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    create_table :some do

    end
    create_table :some_buffers, as: :some, options: "Buffer(#{connection.database}, some, 1, 10, 60, 100, 10000, 10000000, 100000000)"
  end
end

