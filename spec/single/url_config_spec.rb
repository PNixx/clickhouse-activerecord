# frozen_string_literal: true

RSpec.describe 'URL-based configuration' do
  let(:http_connection) { instance_double(Net::HTTP) }
  let(:response) { instance_double(Net::HTTPResponse, code: '200', body: '') }

  before do
    allow(Net::HTTP).to receive(:start).and_return(http_connection)
    allow(http_connection).to receive(:keep_alive_timeout=)
    allow(http_connection).to receive(:started?).and_return(true)
    allow(http_connection).to receive(:post).and_return(response)
  end

  def net_http_start_args
    args = nil
    allow(Net::HTTP).to receive(:start) do |*a, **kw, &block|
      args = { args: a, kwargs: kw }
      block.call(http_connection) if block
      http_connection
    end
    ActiveRecord::Base.clickhouse_connection(config)
    args
  end

  context 'basic URL' do
    let(:config) { { url: 'clickhouse://app_user:secret@db.example.com:9000/mydb' } }

    subject(:adapter) { ActiveRecord::Base.clickhouse_connection(config) }

    it 'sets host from URL' do
      expect(adapter.instance_variable_get(:@connection_parameters)[:host]).to eq('db.example.com')
    end

    it 'sets port from URL' do
      expect(adapter.instance_variable_get(:@connection_parameters)[:port]).to eq(9000)
    end

    it 'sets username from URL' do
      expect(adapter.instance_variable_get(:@connection_config)[:user]).to eq('app_user')
    end

    it 'sets password from URL' do
      expect(adapter.instance_variable_get(:@connection_config)[:password]).to eq('secret')
    end

    it 'sets database from URL path' do
      expect(adapter.instance_variable_get(:@connection_config)[:database]).to eq('mydb')
    end
  end

  context 'URL with special characters in credentials' do
    let(:config) { { url: 'clickhouse://us%40er:p%40ss%3Aword@localhost:8123/testdb' } }

    subject(:adapter) { ActiveRecord::Base.clickhouse_connection(config) }

    it 'decodes username' do
      expect(adapter.instance_variable_get(:@connection_config)[:user]).to eq('us@er')
    end

    it 'decodes password' do
      expect(adapter.instance_variable_get(:@connection_config)[:password]).to eq('p@ss:word')
    end
  end

  context 'URL with query parameters' do
    let(:config) { { url: 'clickhouse://user:pass@localhost:8123/db?ssl=true&debug=true&read_timeout=120&write_timeout=90&keep_alive_timeout=30&http_auth=basic&cluster_name=mycluster' } }

    before do
      allow(http_connection).to receive(:read_timeout=)
      allow(http_connection).to receive(:write_timeout=)
    end

    subject(:adapter) { ActiveRecord::Base.clickhouse_connection(config) }

    it 'sets ssl from query params' do
      expect(adapter.instance_variable_get(:@connection_parameters)[:ssl]).to eq(true)
    end

    it 'sets debug from query params' do
      expect(adapter.instance_variable_get(:@debug)).to eq(true)
    end

    it 'sets read_timeout from query params' do
      expect(adapter.instance_variable_get(:@connection_parameters)[:read_timeout]).to eq(120)
    end

    it 'sets write_timeout from query params' do
      expect(adapter.instance_variable_get(:@connection_parameters)[:write_timeout]).to eq(90)
    end

    it 'sets keep_alive_timeout from query params' do
      expect(adapter.instance_variable_get(:@connection_parameters)[:keep_alive_timeout]).to eq(30)
    end

    it 'sets http_auth from query params' do
      expect(adapter.instance_variable_get(:@http_auth)).to eq(:basic)
    end
  end

  context 'explicit config keys override URL values' do
    let(:config) do
      {
        url: 'clickhouse://url_user:url_pass@url_host:9000/url_db',
        host: 'override_host',
        database: 'override_db',
        username: 'override_user'
      }
    end

    subject(:adapter) { ActiveRecord::Base.clickhouse_connection(config) }

    it 'uses explicit host over URL host' do
      expect(adapter.instance_variable_get(:@connection_parameters)[:host]).to eq('override_host')
    end

    it 'uses explicit database over URL database' do
      expect(adapter.instance_variable_get(:@connection_config)[:database]).to eq('override_db')
    end

    it 'uses explicit username over URL username' do
      expect(adapter.instance_variable_get(:@connection_config)[:user]).to eq('override_user')
    end

    it 'keeps URL password when not overridden' do
      expect(adapter.instance_variable_get(:@connection_config)[:password]).to eq('url_pass')
    end
  end

  context 'URL without port' do
    let(:config) { { url: 'clickhouse://localhost/mydb' } }

    subject(:adapter) { ActiveRecord::Base.clickhouse_connection(config) }

    it 'uses default port 8123' do
      expect(adapter.instance_variable_get(:@connection_parameters)[:port]).to eq(8123)
    end
  end

  context 'URL without database' do
    let(:config) { { url: 'clickhouse://localhost:8123' } }

    it 'raises ArgumentError about missing database' do
      expect { ActiveRecord::Base.clickhouse_connection(config) }
        .to raise_error(ArgumentError, /No database specified/)
    end
  end

  context 'URL with invalid http_auth' do
    let(:config) { { url: 'clickhouse://localhost:8123/db?http_auth=invalid_mode' } }

    it 'raises ArgumentError about unknown http_auth mode' do
      expect { ActiveRecord::Base.clickhouse_connection(config) }
        .to raise_error(ArgumentError, /Unknown :http_auth mode/)
    end
  end

  context 'backward compatibility without url key' do
    let(:config) do
      {
        adapter: 'clickhouse',
        host: 'localhost',
        port: 8123,
        database: 'test_db',
        username: 'user',
        password: 'pass'
      }
    end

    subject(:adapter) { ActiveRecord::Base.clickhouse_connection(config) }

    it 'still works with explicit hash config' do
      expect(adapter.instance_variable_get(:@connection_parameters)[:host]).to eq('localhost')
      expect(adapter.instance_variable_get(:@connection_config)[:database]).to eq('test_db')
    end
  end
end
