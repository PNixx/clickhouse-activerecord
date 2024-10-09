# frozen_string_literal: true

class CreateSomeFunction < ActiveRecord::Migration[7.1]
  def up
    create_function :some_fun, "(x,y) -> x + y"
    create_function :forced_fun, "(x,y) -> x + y", force: true
  end
end
