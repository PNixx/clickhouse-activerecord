# frozen_string_literal: true

class CreateSomeFunction < ActiveRecord::Migration[5.0]
  def up
    execute('CREATE FUNCTION some_fun AS (x,y) -> x + y')
  end
end
