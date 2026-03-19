# frozen_string_literal: true

require 'base64'
require 'uri'

RSpec.describe 'HTTP auth modes' do
  let(:http_connection) { instance_double(Net::HTTP) }
  let(:response_body) { '' }
  let(:response) { instance_double(Net::HTTPResponse, code: '200', body: response_body) }
  let(:base_config) do
    {
      adapter: 'clickhouse',
      host: 'localhost',
      port: 8123,
      database: 'test_db',
      username: 'app_user',
      password: 'secret'
    }
  end
  let(:request_settings) { { max_threads: 1 } }
  let(:target_database) { 'analytics' }
  let(:config) { base_config }
  subject(:adapter) { ActiveRecord::Base.clickhouse_connection(config) }

  before do
    allow(Net::HTTP).to receive(:start).and_return(http_connection)
    allow(http_connection).to receive(:keep_alive_timeout=)
    allow(http_connection).to receive(:started?).and_return(true)
    allow(http_connection).to receive(:post).and_return(response)
  end

  def request_payload_for(settings: request_settings)
    request_payload = {}

    allow(http_connection).to receive(:post) do |path, _body, headers|
      request_payload[:query_params] = query_params(path)
      request_payload[:headers] = headers

      response
    end

    adapter.execute('SELECT 1', settings: settings)
    request_payload
  end

  def query_params(path)
    query = path.split('?', 2).last.to_s
    URI.decode_www_form(query).to_h
  end

  def json_compact_each_row(names:, types:, rows:)
    ([names, types] + rows).map(&:to_json).join("\n")
  end

  context 'default mode' do
    it 'uses url query auth' do
      payload = request_payload_for

      expect(payload[:query_params]).to include(
        'user' => config[:username],
        'password' => config[:password],
        'database' => config[:database],
        'max_threads' => request_settings[:max_threads].to_s
      )
      expect(payload[:headers]).to_not have_key('Authorization')
      expect(payload[:headers]).to_not have_key('X-ClickHouse-User')
    end
  end

  context 'basic auth mode' do
    let(:config) { base_config.merge(http_auth: :basic) }

    it 'uses Authorization header' do
      payload = request_payload_for

      expect(payload[:query_params]).to include(
        'database' => config[:database],
        'max_threads' => request_settings[:max_threads].to_s
      )
      expect(payload[:query_params]).to_not have_key('user')
      expect(payload[:query_params]).to_not have_key('password')
      expect(payload[:headers]['Authorization']).to eq("Basic #{Base64.strict_encode64("#{config[:username]}:#{config[:password]}")}")
    end

    it 'uses Authorization header for create_database' do
      adapter.create_database(target_database)

      expect(http_connection).to have_received(:post) do |path, _body, headers|
        params = query_params(path)

        expect(params).to_not have_key('user')
        expect(params).to_not have_key('password')
        expect(params).to_not have_key('database')
        expect(headers['Authorization']).to eq("Basic #{Base64.strict_encode64("#{config[:username]}:#{config[:password]}")}")
      end
    end

    it 'uses Authorization header for drop_database' do
      adapter.drop_database(target_database)

      expect(http_connection).to have_received(:post) do |path, _body, headers|
        params = query_params(path)

        expect(params).to_not have_key('user')
        expect(params).to_not have_key('password')
        expect(params).to_not have_key('database')
        expect(headers['Authorization']).to eq("Basic #{Base64.strict_encode64("#{config[:username]}:#{config[:password]}")}")
      end
    end

    it 'uses Authorization header for system queries' do
      allow(http_connection).to receive(:post) do |path, _body, headers|
        expect(query_params(path)).to_not have_key('user')
        expect(query_params(path)).to_not have_key('password')
        expect(headers['Authorization']).to eq("Basic #{Base64.strict_encode64("#{config[:username]}:#{config[:password]}")}")
        instance_double(Net::HTTPResponse, code: '200',
                                           body: json_compact_each_row(names: ['name'], types: ['String'], rows: [['events']]))
      end

      expect(adapter.tables).to eq(['events'])
      expect(adapter.views).to eq(['events'])
    end

    context 'mode as string' do
      let(:config) { base_config.merge(http_auth: 'basic') }

      it 'uses Authorization header' do
        payload = request_payload_for

        expect(payload[:query_params]).to_not have_key('user')
        expect(payload[:query_params]).to_not have_key('password')
        expect(payload[:headers]['Authorization']).to eq("Basic #{Base64.strict_encode64("#{config[:username]}:#{config[:password]}")}")
      end
    end

    context 'without username/password' do
      let(:config) { base_config.merge(username: nil, password: nil, http_auth: :basic) }

      it 'does not send Authorization header' do
        payload = request_payload_for

        expect(payload[:query_params]).to include(
          'database' => config[:database],
          'max_threads' => request_settings[:max_threads].to_s
        )
        expect(payload[:query_params]).to_not have_key('user')
        expect(payload[:query_params]).to_not have_key('password')
        expect(payload[:headers]).to_not have_key('Authorization')
      end
    end
  end

  context 'x-clickhouse headers mode' do
    let(:config) { base_config.merge(http_auth: :x_clickhouse_headers) }

    it 'uses X-ClickHouse auth headers' do
      payload = request_payload_for

      expect(payload[:query_params]).to include('max_threads' => request_settings[:max_threads].to_s)
      expect(payload[:query_params]).to_not have_key('user')
      expect(payload[:query_params]).to_not have_key('password')
      expect(payload[:query_params]).to_not have_key('database')

      expect(payload[:headers]).to include(
        'X-ClickHouse-User' => config[:username],
        'X-ClickHouse-Key' => config[:password],
        'X-ClickHouse-Database' => config[:database]
      )
    end

    context 'mode as string' do
      let(:config) { base_config.merge(http_auth: 'x_clickhouse_headers') }

      it 'uses X-ClickHouse auth headers' do
        payload = request_payload_for

        expect(payload[:query_params]).to_not have_key('user')
        expect(payload[:query_params]).to_not have_key('password')
        expect(payload[:query_params]).to_not have_key('database')

        expect(payload[:headers]).to include(
          'X-ClickHouse-User' => config[:username],
          'X-ClickHouse-Key' => config[:password],
          'X-ClickHouse-Database' => config[:database]
        )
      end
    end

    it 'does not include database auth context for create_database' do
      adapter.create_database(target_database)

      expect(http_connection).to have_received(:post) do |_path, _body, headers|
        expect(headers).to_not have_key('X-ClickHouse-Database')

        expect(headers).to include(
          'X-ClickHouse-User' => config[:username],
          'X-ClickHouse-Key' => config[:password]
        )
      end
    end

    it 'does not include database auth context for drop_database' do
      adapter.drop_database(target_database)

      expect(http_connection).to have_received(:post) do |_path, _body, headers|
        expect(headers).to_not have_key('X-ClickHouse-Database')

        expect(headers).to include(
          'X-ClickHouse-User' => config[:username],
          'X-ClickHouse-Key' => config[:password]
        )
      end
    end

    it 'uses auth headers for execute_to_file requests' do
      streaming_response = instance_double(Net::HTTPResponse, code: '200')

      allow(http_connection).to receive(:request) do |request, _sql, &callback|
        params = query_params(request.path)
        expect(params).to include('max_threads' => request_settings[:max_threads].to_s)
        expect(params).to_not have_key('user')
        expect(params).to_not have_key('password')
        expect(params).to_not have_key('database')

        expect(request['X-ClickHouse-User']).to eq(config[:username])
        expect(request['X-ClickHouse-Key']).to eq(config[:password])
        expect(request['X-ClickHouse-Database']).to eq(config[:database])

        allow(streaming_response).to receive(:read_body).and_yield("id\n1\n")
        callback.call(streaming_response)
      end

      file = adapter.execute_to_file('SELECT 1', settings: request_settings)
      expect(file.read).to eq("id\n1\n")
      file.close!
    end

    context 'without username/password' do
      let(:config) { base_config.merge(username: nil, password: nil, http_auth: :x_clickhouse_headers) }

      it 'does not send empty auth headers' do
        payload = request_payload_for

        expect(payload[:query_params]).to include('max_threads' => request_settings[:max_threads].to_s)
        expect(payload[:query_params]).to_not have_key('user')
        expect(payload[:query_params]).to_not have_key('password')
        expect(payload[:query_params]).to_not have_key('database')

        expect(payload[:headers]).to_not have_key('X-ClickHouse-User')
        expect(payload[:headers]).to_not have_key('X-ClickHouse-Key')
        expect(payload[:headers]['X-ClickHouse-Database']).to eq(config[:database])
      end
    end
  end

  context 'invalid mode' do
    it 'raises argument error' do
      expect do
        ActiveRecord::Base.clickhouse_connection(base_config.merge(http_auth: :unsupported))
      end.to raise_error(ArgumentError, /Unknown :http_auth mode/)
    end
  end
end
