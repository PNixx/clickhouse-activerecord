# frozen_string_literal: true

class CreateSomeFunction < ActiveRecord::Migration[5.0]
  def up
    execute 'CREATE FUNCTION multFun AS (x,y) -> x * y'
    execute 'CREATE FUNCTION addFun AS (x,y) -> x + y'
  end
end
