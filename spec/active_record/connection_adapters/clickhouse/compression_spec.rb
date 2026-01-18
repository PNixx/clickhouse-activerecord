# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::Clickhouse::Compression do
  let(:test_data) { 'SELECT * FROM test_table WHERE id = 1' * 100 }

  describe '.compress' do
    context 'with gzip' do
      it 'compresses data' do
        expect(described_class).to receive(:gzip_compress).and_call_original
        compressed = described_class.compress(test_data, 'gzip')
        expect(compressed).not_to eq(test_data)
        expect(compressed.bytesize).to be < test_data.bytesize
      end
    end
    
    context 'with brotli' do
      it 'compresses data' do
        expect(described_class).to receive(:brotli_compress).and_call_original
        compressed = described_class.compress(test_data, 'br')
        expect(compressed).not_to eq(test_data)
        expect(compressed.bytesize).to be < test_data.bytesize
      end
    end
    
    context "when the method is not implemented" do
      it "returns the data" do
        compressed = described_class.compress(test_data, 'other')
        expect(compressed.bytesize).to eq(test_data.bytesize)
        expect(compressed).to eq(test_data)
      end
    end
  end

  describe '.decompress' do
    context 'with gzip' do
      it 'decompresses data' do
        compressed = described_class.compress(test_data, 'gzip')
        decompressed = described_class.decompress(compressed, 'gzip')
        expect(decompressed).to eq(test_data)
      end

      it 'returns data as-is when already decompressed (no gzip magic bytes)' do
        # Simulates Net::HTTP auto-decompression behavior
        decompressed = described_class.decompress(test_data, 'gzip')
        expect(decompressed).to eq(test_data)
      end
    end

    context 'with deflate' do
      it 'decompresses data' do
        compressed = described_class.compress(test_data, 'deflate')
        decompressed = described_class.decompress(compressed, 'deflate')
        expect(decompressed).to eq(test_data)
      end

      it 'returns data as-is when already decompressed (no deflate magic byte)' do
        # Simulates Net::HTTP auto-decompression behavior
        decompressed = described_class.decompress(test_data, 'deflate')
        expect(decompressed).to eq(test_data)
      end
    end

    context 'with brotli' do
      it 'decompresses data' do
        compressed = described_class.compress(test_data, 'br')
        decompressed = described_class.decompress(compressed, 'br')
        expect(decompressed).to eq(test_data)
      end
    end

    context 'when the method is not implemented' do
      it 'returns the compressed data' do
        compressed = described_class.compress(test_data, 'gzip')
        decompressed = described_class.decompress(compressed, 'other')
        expect(decompressed).not_to eq(test_data)
      end
    end
  end
end
