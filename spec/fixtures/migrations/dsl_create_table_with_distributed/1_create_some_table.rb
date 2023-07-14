class CreateSomeTable < ActiveRecord::Migration[5.0]
  def change
    opts = <<~SQL.squish
      MergeTree
      PARTITION BY date
      ORDER BY (date)
      SETTINGS index_granularity = 8192
    SQL
    create_table :some_distributed, id: false, with_distributed: :some, options: opts do |t|
      t.date :date, null: false
    end
  end
end
