# frozen_string_literal: true

RSpec.describe 'Connection hardening' do
  let(:http_connection) { instance_double(Net::HTTP) }
  let(:success_body) { %(["result"]\n["UInt8"]\n[1]\n) }
  let(:success_response) { instance_double(Net::HTTPResponse, code: '200', body: success_body) }

  before do
    allow(http_connection).to receive(:keep_alive_timeout=)
    allow(http_connection).to receive(:read_timeout=)
    allow(http_connection).to receive(:write_timeout=)
    allow(http_connection).to receive(:ca_file=)
    allow(http_connection).to receive(:started?).and_return(true)
    allow(http_connection).to receive(:finish)
  end

  describe 'connect' do
    def net_http_start_args
      args = nil
      allow(Net::HTTP).to receive(:start) do |*a, **kw|
        args = { args: a, kwargs: kw }
        http_connection
      end
      ActiveRecord::Base.clickhouse_connection(config)
      args
    end

    context 'without open_timeout configured' do
      let(:config) { { adapter: 'clickhouse', host: 'localhost', database: 'db' } }

      it 'does not pass open_timeout to Net::HTTP.start, preserving its own default' do
        expect(net_http_start_args[:kwargs]).not_to have_key(:open_timeout)
      end
    end

    context 'with open_timeout configured' do
      let(:config) { { adapter: 'clickhouse', host: 'localhost', database: 'db', open_timeout: 5 } }

      it 'passes it through to Net::HTTP.start' do
        expect(net_http_start_args[:kwargs][:open_timeout]).to eq(5)
      end
    end

    context 'by default' do
      let(:config) { { adapter: 'clickhouse', host: 'localhost', database: 'db' } }

      it 'does not verify the TLS certificate, matching prior behavior' do
        expect(net_http_start_args[:kwargs][:verify_mode]).to eq(OpenSSL::SSL::VERIFY_NONE)
      end
    end

    context 'with insecure: false' do
      let(:config) { { adapter: 'clickhouse', host: 'localhost', database: 'db', insecure: false } }

      it 'verifies the TLS certificate' do
        expect(net_http_start_args[:kwargs][:verify_mode]).to eq(OpenSSL::SSL::VERIFY_PEER)
      end
    end

    context 'with sslca configured' do
      let(:config) { { adapter: 'clickhouse', host: 'localhost', database: 'db', sslca: '/path/to/ca.pem' } }

      it 'sets ca_file on the connection' do
        net_http_start_args
        expect(http_connection).to have_received(:ca_file=).with('/path/to/ca.pem')
      end
    end
  end

  describe 'session settings' do
    let(:config) do
      {
        adapter: 'clickhouse',
        host: 'localhost',
        database: 'db',
        max_execution_time: 25,
        cancel_http_readonly_queries_on_client_close: true
      }
    end

    subject(:adapter) { ActiveRecord::Base.clickhouse_connection(config) }

    before { allow(Net::HTTP).to receive(:start).and_return(http_connection) }

    it 'merges max_execution_time into the request params sent with every query' do
      expect(adapter.instance_variable_get(:@connection_config)[:max_execution_time]).to eq(25)
    end

    it 'merges cancel_http_readonly_queries_on_client_close into the request params' do
      expect(adapter.instance_variable_get(:@connection_config)[:cancel_http_readonly_queries_on_client_close]).to eq(true)
    end
  end

  describe '#exec_query' do
    let(:config) { { adapter: 'clickhouse', host: 'localhost', database: 'db' } }

    subject(:adapter) { ActiveRecord::Base.clickhouse_connection(config) }

    before { allow(Net::HTTP).to receive(:start).and_return(http_connection) }

    context 'on a fast connection-level failure' do
      it 'retries once on a fresh connection and succeeds' do
        call_count = 0
        allow(http_connection).to receive(:post) do
          call_count += 1
          call_count == 1 ? raise(Errno::ECONNRESET) : success_response
        end

        result = adapter.exec_query('SELECT 1')

        expect(call_count).to eq(2)
        expect(result.rows).to eq([[1]])
      end

      it 'only retries once, then raises ActiveRecord::ConnectionFailed' do
        allow(http_connection).to receive(:post).and_raise(EOFError)

        expect { adapter.exec_query('SELECT 1') }.to raise_error(ActiveRecord::ConnectionFailed)
        expect(http_connection).to have_received(:post).twice
      end
    end

    context 'when the connection fails to open, without retry_on_connect_error' do
      it 'does not retry a refused connection' do
        allow(http_connection).to receive(:post).and_raise(Errno::ECONNREFUSED)

        expect { adapter.exec_query('SELECT 1') }.to raise_error(ActiveRecord::ActiveRecordError, /refused/)
        expect(http_connection).to have_received(:post).once
      end

      it 'retries a read that times out opening once, then raises ActiveRecord::ConnectionFailed' do
        allow(http_connection).to receive(:post).and_raise(Net::OpenTimeout)

        expect { adapter.exec_query('SELECT 1') }.to raise_error(ActiveRecord::ConnectionFailed)
        expect(http_connection).to have_received(:post).twice
      end
    end

    context 'when retry_on_connect_error is the string "false", as Rails passes it from a URL' do
      let(:config) { { adapter: 'clickhouse', host: 'localhost', database: 'db', retry_on_connect_error: 'false' } }

      it 'does not retry a refused connection' do
        allow(http_connection).to receive(:post).and_raise(Errno::ECONNREFUSED)

        expect { adapter.exec_query('SELECT 1') }.to raise_error(ActiveRecord::ActiveRecordError, /refused/)
        expect(http_connection).to have_received(:post).once
      end
    end

    context 'with retry_on_connect_error' do
      let(:config) { { adapter: 'clickhouse', host: 'localhost', database: 'db', retry_on_connect_error: true } }

      [Net::OpenTimeout, SocketError, Errno::ECONNREFUSED].each do |error|
        context "when the connection fails to open (#{error})" do
          it 'retries once and succeeds' do
            call_count = 0
            allow(http_connection).to receive(:post) do
              call_count += 1
              call_count == 1 ? raise(error) : success_response
            end

            expect(adapter.exec_query('SELECT 1').rows).to eq([[1]])
            expect(call_count).to eq(2)
          end

          it 'only retries once, then raises ActiveRecord::ConnectionNotEstablished' do
            allow(http_connection).to receive(:post).and_raise(error)

            expect { adapter.exec_query('SELECT 1') }.to raise_error(ActiveRecord::ConnectionNotEstablished)
            expect(http_connection).to have_received(:post).twice
          end
        end
      end

      context 'when a started session fails to reopen its socket' do
        before do
          started = true
          allow(http_connection).to receive(:started?) { started }
          allow(http_connection).to receive(:finish) { started = false }
          allow(http_connection).to receive(:start) { started = true }
        end

        it 'finishes the session and retries on a new one' do
          call_count = 0
          allow(http_connection).to receive(:post) do
            call_count += 1
            call_count == 1 ? raise(Errno::ECONNREFUSED) : success_response
          end

          expect(adapter.exec_query('SELECT 1').rows).to eq([[1]])
          expect(http_connection).to have_received(:finish).ordered
          expect(http_connection).to have_received(:start).once.ordered
        end
      end

      context 'when reopening a finished session fails to open' do
        before do
          started = false
          start_count = 0
          allow(http_connection).to receive(:started?) { started }
          allow(http_connection).to receive(:start) do
            start_count += 1
            raise Errno::ECONNREFUSED if start_count == 1

            started = true
          end
        end

        it 'retries a query once and succeeds' do
          allow(http_connection).to receive(:post).and_return(success_response)

          expect(adapter.exec_query('SELECT 1').rows).to eq([[1]])
          expect(http_connection).to have_received(:start).twice
        end

        it 'retries a streamed query once and succeeds' do
          streaming_response = instance_double(Net::HTTPResponse, code: '200')
          allow(streaming_response).to receive(:read_body).and_yield("id\n1\n")
          allow(http_connection).to receive(:request) { |_request, _sql, &callback| callback.call(streaming_response) }

          file = adapter.execute_to_file('SELECT 1')

          expect(file.read).to eq("id\n1\n")
          expect(http_connection).to have_received(:start).twice
          file.close!
        end
      end
    end

    context 'on Net::ReadTimeout' do
      it 'does not retry and raises ActiveRecord::AdapterTimeout instead' do
        allow(http_connection).to receive(:post).and_raise(Net::ReadTimeout)

        expect { adapter.exec_query('SELECT 1') }.to raise_error(ActiveRecord::AdapterTimeout)
        expect(http_connection).to have_received(:post).once
      end
    end

    context 'when the server cancels the query via max_execution_time' do
      it 'raises ActiveRecord::StatementTimeout' do
        response = instance_double(
          Net::HTTPResponse,
          code: '500',
          body: 'Code: 159. DB::Exception: Timeout exceeded (TIMEOUT_EXCEEDED)'
        )
        allow(http_connection).to receive(:post).and_return(response)

        expect { adapter.exec_query('SELECT 1') }.to raise_error(ActiveRecord::StatementTimeout)
      end
    end
  end

  describe '#exec_insert_all' do
    let(:config) { { adapter: 'clickhouse', host: 'localhost', database: 'db' } }
    let(:insert) { 'INSERT INTO t (id) VALUES (1)' }
    let(:insert_response) { instance_double(Net::HTTPResponse, code: '200', body: '') }

    subject(:adapter) { ActiveRecord::Base.clickhouse_connection(config) }

    before { allow(Net::HTTP).to receive(:start).and_return(http_connection) }

    [Net::OpenTimeout, Errno::ECONNREFUSED].each do |error|
      context "when the connection fails to open (#{error}), without retry_on_connect_error" do
        it 'does not retry' do
          allow(http_connection).to receive(:post).and_raise(error)

          expect { adapter.exec_insert_all(insert, 'Bulk Insert') }.to raise_error(error)
          expect(http_connection).to have_received(:post).once
        end
      end
    end

    context 'with retry_on_connect_error' do
      let(:config) { { adapter: 'clickhouse', host: 'localhost', database: 'db', retry_on_connect_error: true } }

      [Net::OpenTimeout, SocketError, Errno::ECONNREFUSED].each do |error|
        context "when the connection fails to open (#{error})" do
          it 'retries once and succeeds' do
            call_count = 0
            allow(http_connection).to receive(:post) do
              call_count += 1
              call_count == 1 ? raise(error) : insert_response
            end

            expect(adapter.exec_insert_all(insert, 'Bulk Insert')).to be(true)
            expect(call_count).to eq(2)
          end

          it 'only retries once, then raises ActiveRecord::ConnectionNotEstablished' do
            allow(http_connection).to receive(:post).and_raise(error)

            expect { adapter.exec_insert_all(insert, 'Bulk Insert') }.to raise_error(ActiveRecord::ConnectionNotEstablished)
            expect(http_connection).to have_received(:post).twice
          end
        end
      end

      [EOFError, Errno::ECONNRESET, IOError].each do |error|
        context "when an open connection drops (#{error})" do
          it 'does not retry, so the server cannot apply the insert twice' do
            allow(http_connection).to receive(:post).and_raise(error)

            expect { adapter.exec_insert_all(insert, 'Bulk Insert') }.to raise_error(error)
            expect(http_connection).to have_received(:post).once
          end
        end
      end
    end
  end
end
