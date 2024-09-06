# frozen_string_literal: true

class CreateSomeFunction < ActiveRecord::Migration[7.1]
  def up
    sql = <<~SQL
      CREATE FUNCTION multFun AS (x,y) -> x * y
    SQL
    do_execute(sql, format: nil)
    
    sql = <<~SQL
      CREATE FUNCTION addFun AS (x,y) -> x + y
    SQL
    execute(sql, format: nil)
  end
end
