class CreateSomeTable1 < ActiveRecord::Migration[7.1]
  def change
    create_table :some_table_1, options: 'MergeTree ORDER BY col' do |t|
      t.string :col, null: false
    end
  end
end
