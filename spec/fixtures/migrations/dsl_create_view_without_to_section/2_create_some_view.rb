class CreateSomeView < ActiveRecord::Migration[5.0]
  def change
    create_view :some_view, materialized: true, as: 'select * from some_table'
  end
end
