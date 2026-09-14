# frozen_string_literal: true

RSpec.describe 'ActiveRecord::ConnectionAdapters::ClickhouseAdapter' do
  let(:connection) { ActiveRecord::Base.connection }

  describe 'query cache' do
    let(:count) { 'SELECT count() FROM query_cache_test' }

    before do
      connection.execute('CREATE TABLE query_cache_test (id UInt64) ENGINE = MergeTree ORDER BY id')
    end

    after do
      connection.execute('DROP TABLE IF EXISTS query_cache_test')
    end

    it 'drops the cached reads when a statement writes' do
      ActiveRecord::Base.cache do
        expect(connection.select_value(count)).to eq(0)

        connection.execute('INSERT INTO query_cache_test SELECT 1')

        expect(connection.select_value(count)).to eq(1)
      end
    end
  end
end
