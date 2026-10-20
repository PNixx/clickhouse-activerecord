class CreateSomeView < ActiveRecord::Migration[7.1]
  def change
    create_view :some_view, as: 'select * from some_table_1'
  end
end
