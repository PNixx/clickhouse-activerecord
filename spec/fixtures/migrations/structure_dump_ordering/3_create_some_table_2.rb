class CreateSomeTable2 < ActiveRecord::Migration[7.1]
  def change
    create_table :some_table_2, options: 'MergeTree ORDER BY col' do |t|
      t.string :col, null: false
    end
  end
end
