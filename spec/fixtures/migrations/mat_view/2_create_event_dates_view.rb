# frozen_string_literal: true

class CreateEventDatesView < ActiveRecord::Migration[5.0]
  def up
    ActiveRecord::Base.connection.execute(<<~SQL.squish)
      CREATE MATERIALIZED VIEW event_dates
      ENGINE = AggregatingMergeTree()
      ORDER BY date
      AS SELECT date,
                sumState(id) AS ids
      FROM events
      GROUP BY date
    SQL
  end
end
