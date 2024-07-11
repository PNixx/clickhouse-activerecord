# frozen_string_literal: true

class CreateSomeTable < ActiveRecord::Migration[7.1]
  def up
    execute <<~SQL
      CREATE TABLE some (
        id                               UInt64,
        date                             Date
      ) ENGINE = Memory
    SQL
  end
end
