# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::Clickhouse::Statement::ResponseProcessor do
  let(:sql) { 'SELECT 1' }
  let(:format) { 'JSONCompact' }

  def build_response(code:, body:, content_encoding: nil)
    response = Net::HTTPResponse.new('1.1', code, '')
    response.instance_variable_set(:@body, body)
    response.instance_variable_set(:@read, true)
    response['Content-Encoding'] = content_encoding if content_encoding
    response
  end

  describe '#process' do
    context 'when the format is JSONCompact' do
      it 'returns a parsed JSON response' do
        body = '{"meta":[{"name":"1","type":"UInt8"}],"data":[[1]],"rows":1}'
        response = build_response(code: 200, body: body)
  
        processor = described_class.new(response, format, sql)
        result = processor.process
  
        expect(result).to eq({
          'meta' => [{ 'name' => '1', 'type' => 'UInt8' }],
          'data' => [[1]],
          'rows' => 1
        })
      end
    end
    
    context 'when the format is JSONCompactEachRowWithNamesAndTypes' do
      let(:format) { 'JSONCompactEachRowWithNamesAndTypes' }
      
      it 'returns a parsed JSON response' do
        body = "[\"id\",\"name\"]\n[\"UInt64\",\"String\"]\n[1,\"test\"]\n[2,\"another\"]"
        response = build_response(code: 200, body: body)

        processor = described_class.new(response, format, sql)
        result = processor.process

        expect(result).to eq({
          'meta' => [
            { 'name' => 'id', 'type' => 'UInt64' },
            { 'name' => 'name', 'type' => 'String' }
          ],
          'data' => [[1, 'test'], [2, 'another']]
        })
      end
    end

    context 'when the format is not implemented' do
      let(:format) { 'Other' }
      
      it 'returns the raw body' do
        body = "1\tvalue\n2\tanother"
        response = build_response(code: 200, body: body)
  
        processor = described_class.new(response, format, sql)
        result = processor.process
  
        expect(result).to eq(body)
      end
    end

    it 'returns empty body as-is' do
      response = build_response(code: 200, body: '')

      processor = described_class.new(response, format, sql)
      result = processor.process

      expect(result).to eq('')
    end
    
    context 'when decompression is requested' do
      context 'with gzip compression' do
        it 'has been decompressed by Net::HTTP and returns the body' do
          json_body = '{"meta":[{"name":"1","type":"UInt8"}],"data":[[1]],"rows":1}'
          response = build_response(code: 200, body: json_body, content_encoding: 'gzip')
  
          processor = described_class.new(response, format, sql)
          result = processor.process
  
          expect(result).to eq({
            'meta' => [{ 'name' => '1', 'type' => 'UInt8' }],
            'data' => [[1]],
            'rows' => 1
          })
        end
      end
      context 'with brotli compression' do
        it 'decompresses and returns the body' do
          json_body = '{"meta":[{"name":"1","type":"UInt8"}],"data":[[1]],"rows":1}'
          compressed_body = Brotli.deflate(json_body)
          response = build_response(code: 200, body: compressed_body, content_encoding: 'br')
  
          processor = described_class.new(response, format, sql)
          result = processor.process
  
          expect(result).to eq({
            'meta' => [{ 'name' => '1', 'type' => 'UInt8' }],
            'data' => [[1]],
            'rows' => 1
          })
        end
      end
    end

    context 'when success? returns false' do
      it 'raises NoDatabaseError for UNKNOWN_DATABASE exception' do
        body = "Code: 81. DB::Exception: Database test_db does not exist. (UNKNOWN_DATABASE)"
        response = build_response(code: 404, body: body)

        processor = described_class.new(response, format, sql)

        expect { processor.process }.to raise_error(ActiveRecord::NoDatabaseError)
      end

      it 'raises DatabaseAlreadyExists for DATABASE_ALREADY_EXISTS exception' do
        body = "Code: 82. DB::Exception: Database test_db already exists. (DATABASE_ALREADY_EXISTS)"
        response = build_response(code: 500, body: body)

        processor = described_class.new(response, format, sql)

        expect { processor.process }.to raise_error(ActiveRecord::DatabaseAlreadyExists)
      end

      it 'raises ActiveRecordError for generic database errors' do
        body = "Code: 62. DB::Exception: Syntax error: failed at position 0. (SYNTAX_ERROR)"
        response = build_response(code: 400, body: body)

        processor = described_class.new(response, format, sql)

        expect { processor.process }.to raise_error(ActiveRecord::ActiveRecordError, /Response code: 400/)
      end
    end

    context 'with DB::Exception in successful response' do
      it 'raises ActiveRecordError when body contains DB::Exception pattern' do
        body = "Code: 62. DB::Exception: Something went wrong"
        response = build_response(code: 200, body: body)

        processor = described_class.new(response, format, sql)

        expect { processor.process }.to raise_error(ActiveRecord::ActiveRecordError, /Query: SELECT 1/)
      end
    end
  end
end
