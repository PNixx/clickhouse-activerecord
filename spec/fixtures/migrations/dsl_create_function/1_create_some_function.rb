# frozen_string_literal: true

class CreateSomeFunction < ActiveRecord::Migration[5.0]
  def up
    create_function :some_fun, "(x,y) -> x + y"
  end
end
