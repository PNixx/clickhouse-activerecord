class CreateSomeTable2 < ActiveRecord::Migration[5.0]
  def change
    create_table :some_table_2, options: 'MergeTree() ORDER BY col' do |t|
      t.string :col, null: false
    end
  end
end
