# frozen_string_literal: true

# Regression coverage for the @response_format thread-safety race.
#
# `with_response_format` mutates an instance variable on the adapter to scope
# the response format for the duration of a block. When the same adapter is
# shared across threads — as happens in Capybara system specs where the test
# thread and the Puma server thread both check out the same connection — one
# thread's block can leak its format into another thread's `execute` call,
# returning the raw response body instead of a parsed Hash.
RSpec.describe 'ActiveRecord::ConnectionAdapters::Clickhouse::SchemaStatements response format thread safety' do
  let(:connection) { ActiveRecord::Base.connection }
  let(:default_format) { ActiveRecord::ConnectionAdapters::ClickhouseAdapter::DEFAULT_RESPONSE_FORMAT }

  before do
    connection.execute('CREATE TABLE response_format_thread_test (id UInt64) ENGINE = MergeTree ORDER BY id')
  end

  after do
    connection.execute('DROP TABLE IF EXISTS response_format_thread_test')
  end

  it '#execute on one thread is not affected by with_response_format(nil) on another thread' do
    inside_block = Queue.new
    release = Queue.new

    holder = Thread.new do
      connection.with_response_format(nil) do
        inside_block << true
        release.pop
      end
    end

    inside_block.pop

    begin
      result = connection.execute('SELECT count() AS c FROM response_format_thread_test')

      expect(result).to be_a(Hash), "expected parsed Hash from default format, got #{result.class}: #{result.inspect[0..200]}"
      expect(result['data']).to be_a(Array)
    ensure
      release << true
      holder.join
    end
  end

  it 'with_response_format nests correctly within a single thread' do
    expect(connection.execute('SELECT 1 AS x')).to be_a(Hash)

    connection.with_response_format(nil) do
      expect(connection.execute('SELECT 1 AS x')).to be_a(String)

      connection.with_response_format('JSONCompact') do
        expect(connection.execute('SELECT 1 AS x')).to be_a(Hash)
      end

      expect(connection.execute('SELECT 1 AS x')).to be_a(String)
    end

    expect(connection.execute('SELECT 1 AS x')).to be_a(Hash)
  end
end
