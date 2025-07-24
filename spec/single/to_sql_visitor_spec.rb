# frozen_string_literal: true

require 'spec_helper'

RSpec.describe 'ToSql visitor', :migrations do
  let(:model) do
    Class.new(ActiveRecord::Base) do
      self.table_name = 'events'
    end
  end

  before do
    migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'mat_view')
    quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
  end

  context 'when given a basic query' do
    it 'generates the correct SQL' do
      sql_trap = SqlCapture.new { model.select(:id).where(date: '2021-09-22').limit(10).load }
      expect(sql_trap.captured).to eq("SELECT events.id FROM events WHERE events.date = '2021-09-22' LIMIT 10")
    end
  end

  context 'when given an aggregation query' do
    it 'generates the correct SQL' do
      sql_trap = SqlCapture.new { model.where(date: '2021-09-22').count }
      expect(sql_trap.captured).to eq("SELECT COUNT(*) FROM events WHERE events.date = '2021-09-22'")
    end

    context 'when the expression to be aggregated is not the wildcard (*)' do
      it 'generates the correct SQL' do
        sql_trap = SqlCapture.new { model.select(:id).where(date: '2021-09-22').count }
        expect(sql_trap.captured).to eq("SELECT COUNT(events.id) FROM events WHERE events.date = '2021-09-22'")
      end

      context 'when the expression is an Arel node' do
        it 'generates the correct SQL' do
          sql_trap = SqlCapture.new { model.select(model.arel_table[:date]).where(date: '2021-09-22').count }
          expect(sql_trap.captured).to eq("SELECT COUNT(events.date) FROM events WHERE events.date = '2021-09-22'")
        end

        context 'when the relation is a Clickhouse view' do
          let(:mat_view_model) do
            Class.new(ActiveRecord::Base) do
              self.is_view = true
              self.table_name = 'event_dates'
            end
          end

          it 'generates the correct SQL (by suffixing the aggregate function name!)' do
            sql_trap = SqlCapture.new { mat_view_model.where(date: '2021-09-22').sum(:ids) }
            expect(sql_trap.captured).to eq("SELECT sumMerge(event_dates.ids) FROM event_dates WHERE event_dates.date = '2021-09-22'")
          end
        end
      end
    end
  end

  context 'when given a query with inline settings' do
    it 'passes the settings into the SQL' do
      sql = model.settings(max_insert_block_size: 10, use_skip_indexes: true).to_sql
      expect(sql).to eq("SELECT events.* FROM events SETTINGS max_insert_block_size = 10, use_skip_indexes = TRUE")
    end

    it 'sanitizes non-word characters in setting names' do
      sql = model.settings('max_ insert_block_ size:' => 10, 'use_skip_indexes!' => true).to_sql
      expect(sql).to eq("SELECT events.* FROM events SETTINGS max_insert_block_size = 10, use_skip_indexes = TRUE")
    end

    it 'permits arbitrary setting names if given as SqlLiterals (!)' do
      sql = model.settings(Arel.sql('max_ insert_block_ size:') => 10, Arel.sql('use_skip_indexes!') => true).to_sql
      expect(sql).to eq("SELECT events.* FROM events SETTINGS max_ insert_block_ size: = 10, use_skip_indexes! = TRUE")
    end

    it 'quotes setting values' do
      sql = model.settings(insert_deduplication_token: :foo_bar_123).to_sql
      expect(sql).to eq("SELECT events.* FROM events SETTINGS insert_deduplication_token = 'foo_bar_123'")
    end

    it 'allows passing the symbol :default to reset a setting' do
      sql = model.settings(max_insert_block_size: :default).to_sql
      expect(sql).to eq("SELECT events.* FROM events SETTINGS max_insert_block_size = DEFAULT")
    end
  end
end