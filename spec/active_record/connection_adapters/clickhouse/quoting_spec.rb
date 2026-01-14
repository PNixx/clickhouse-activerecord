# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::Clickhouse::Quoting do
  let(:connection) { ActiveRecord::Base.connection }

  describe '.quote_column_name' do
    it 'quotes simple column names with backticks' do
      [connection, connection.class].each do |adapter|
        expect(adapter.quote_column_name('foo')).to eq('`foo`')
      end
    end

    it 'escapes backticks in column names' do
      [connection, connection.class].each do |adapter|
        expect(adapter.quote_column_name('he`llo')).to eq('`he``llo`')
      end
    end

    it 'handles double quotes in column names' do
      [connection, connection.class].each do |adapter|
        expect(adapter.quote_column_name('hel"lo')).to eq('`hel"lo`')
      end
    end
  end

  describe '.quote_table_name' do
    it 'quotes simple table names with backticks' do
      [connection, connection.class].each do |adapter|
        expect(adapter.quote_table_name('foo')).to eq('`foo`')
      end
    end

    it 'handles database.table syntax' do
      [connection, connection.class].each do |adapter|
        expect(adapter.quote_table_name('foo.bar')).to eq('`foo`.`bar`')
      end
    end

    it 'escapes backticks in table names' do
      [connection, connection.class].each do |adapter|
        expect(adapter.quote_table_name('he`llo')).to eq('`he``llo`')
      end
    end

    it 'handles complex names with dots and special characters' do
      [connection, connection.class].each do |adapter|
        expect(adapter.quote_table_name('hel"lo.wor\\ld')).to eq('`hel"lo`.`wor\\ld`')
      end
    end
  end
end
