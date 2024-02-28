class CreateSomeTable < ActiveRecord::Migration[5.0]
  def change
    create_table :some_distributed, with_distributed: :some, id: false, options: 'MergeTree PARTITION BY toYYYYMM(date) ORDER BY (date)' do |t|
      t.date :date, null: false
    end
  end
end
