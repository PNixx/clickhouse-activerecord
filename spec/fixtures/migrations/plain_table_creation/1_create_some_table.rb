
class CreateSomeTable < ActiveRecord::Migration[5.0]
  def up
    execute <<~SQL
      CREATE TABLE some (
        id                               UInt64,
        date                             Date
      ) ENGINE = Memory
    SQL
  end
end
