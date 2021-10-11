class CreateSomeTable < ActiveRecord::Migration[5.0]
  def change
    create_table :some_table, options: 'MergeTree() ORDER BY col' do |t|
      t.string :col, null: false
    end
  end
end
