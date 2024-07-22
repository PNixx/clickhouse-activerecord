class CreateSomeTable < ActiveRecord::Migration[7.1]
  def change
    create_table :some, id: false, options: 'MergeTree ORDER BY col' do |t|
      t.string :col, null: false
    end
  end
end
