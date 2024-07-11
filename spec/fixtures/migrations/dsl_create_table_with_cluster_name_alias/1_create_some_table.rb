class CreateSomeTable < ActiveRecord::Migration[7.1]
  def change
    create_table :some, options: 'MergeTree PARTITION BY toYYYYMM(date) ORDER BY (date)', sync: true, id: false do |t|
      t.date :date, null: false
    end
  end
end
