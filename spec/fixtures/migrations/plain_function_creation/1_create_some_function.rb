# frozen_string_literal: true

class CreateSomeFunction < ActiveRecord::Migration[7.1]
  def up
    sql = <<~SQL
      CREATE FUNCTION some_fun AS (x,y) -> x + y
    SQL
    do_execute(sql, format: nil)
  end
end
