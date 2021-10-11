class CreateSomeTable < ActiveRecord::Migration[5.0]
  def change
    create_table_with_distributed :some, options: 'MergeTree(date, (date), 8192)' do |t|
      t.date :date, null: false
    end
  end
end
