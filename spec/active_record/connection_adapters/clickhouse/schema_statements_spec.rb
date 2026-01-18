# frozen_string_literal: true

RSpec.describe 'ActiveRecord::ConnectionAdapters::Clickhouse::SchemaStatements' do
  let(:connection) { ActiveRecord::Base.connection }

  describe '#execute' do
    it 'retuns a result' do
      result = connection.execute('SELECT 1 AS value')

      expect(result['data']).to eq([[1]])
    end

    context 'with request compression' do
      around do |example|
        original = connection.instance_variable_get(:@request_compression)
        connection.instance_variable_set(:@request_compression, 'gzip')
        example.run
      ensure
        connection.instance_variable_set(:@request_compression, original)
      end

      it 'compresses the request body' do        
        expect(ActiveRecord::ConnectionAdapters::Clickhouse::Compression).to receive(:compress)
          .with('SELECT 1 AS value FORMAT JSONCompactEachRowWithNamesAndTypes', 'gzip')
          .and_call_original

        result = connection.execute('SELECT 1 AS value')
  
        expect(result['data']).to eq([[1]])
      end

      it 'sets Content-Encoding header' do
        http_connection = connection.instance_variable_get(:@connection)

        expect(http_connection).to receive(:request).and_wrap_original do |original, req, body, &block|
          expect(req['Content-Encoding']).to eq('gzip')
          original.call(req, body, &block)
        end

        connection.execute('SELECT 1')
      end
    end

    context 'with response compression' do
      around do |example|
        original = connection.instance_variable_get(:@response_compression)
        connection.instance_variable_set(:@response_compression, 'gzip')
        example.run
      ensure
        connection.instance_variable_set(:@response_compression, original)
      end

      it 'sets Accept-Encoding header' do
        http_connection = connection.instance_variable_get(:@connection)

        expect(http_connection).to receive(:request).and_wrap_original do |original, req, body, &block|
          expect(req['Accept-Encoding']).to eq('gzip')
          original.call(req, body, &block)
        end

        connection.execute('SELECT 1')
      end

      it 'does not set Accept-Encoding header when streaming' do
        http_connection = connection.instance_variable_get(:@connection)

        expect(http_connection).to receive(:request).and_wrap_original do |original, req, body, &block|
          # Net::HTTP sets a default Accept-Encoding, but we should not set our custom 'gzip' value
          expect(req['Accept-Encoding']).not_to eq('gzip')
          original.call(req, body, &block)
        end

        connection.execute_streaming('SELECT 1')
      end
    end
  end

  describe '#truncate_tables' do
    before do
      connection.execute('CREATE TABLE truncate_test (id UInt64, name String) ENGINE = MergeTree ORDER BY id')
    end

    after do
      connection.execute('DROP DICTIONARY IF EXISTS truncate_test_dict')
      connection.execute('DROP TABLE IF EXISTS truncate_test')
      connection.execute('DROP TABLE IF EXISTS truncate_test2')
      connection.execute('DROP VIEW IF EXISTS truncate_test_view')
    end

    it 'truncates multiple tables' do
      connection.execute('CREATE TABLE truncate_test2 (id UInt64, value Int32) ENGINE = MergeTree ORDER BY id')
      connection.exec_insert("INSERT INTO truncate_test (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
      connection.exec_insert("INSERT INTO truncate_test2 (id, value) VALUES (1, 100), (2, 200)")

      expect(connection.select_value('SELECT count() FROM truncate_test')).to eq(2)
      expect(connection.select_value('SELECT count() FROM truncate_test2')).to eq(2)

      connection.truncate_tables('truncate_test', 'truncate_test2')

      expect(connection.select_value('SELECT count() FROM truncate_test')).to eq(0)
      expect(connection.select_value('SELECT count() FROM truncate_test2')).to eq(0)
    end

    it 'skips tables with unsupported engines' do
      connection.execute('CREATE VIEW truncate_test_view AS SELECT * FROM truncate_test')
      connection.execute(<<~SQL)
        CREATE DICTIONARY truncate_test_dict (
          id UInt64,
          name String
        )
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'truncate_test'))
        LIFETIME(MIN 0 MAX 0)
        LAYOUT(FLAT())
      SQL
      connection.exec_insert("INSERT INTO truncate_test (id, name) VALUES (1, 'Alice')")

      expect { connection.truncate_tables(*connection.tables) }.not_to raise_error

      expect(connection.select_value('SELECT count() FROM truncate_test')).to eq(0)
    end
  end
end
