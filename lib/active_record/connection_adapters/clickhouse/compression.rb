# frozen_string_literal: true

require 'zlib'
require 'stringio'

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Compression
        # @see https://clickhouse.com/docs/interfaces/http#compression for supported list
        SUPPORTED_METHODS = Set.new(%w[gzip deflate br zstd lz4 bz2 xz]).freeze

        STDLIB_METHODS = %w[gzip deflate].freeze

        GEM_REQUIREMENTS = {
          'br' => { gem: 'brotli', require: 'brotli' },
          'zstd' => { gem: 'zstd-ruby', require: 'zstd-ruby' },
          'lz4' => { gem: 'extlz4', require: 'extlz4' },
          'bz2' => { gem: 'bzip2-ffi', require: 'bzip2/ffi' },
          'xz' => { gem: 'ruby-xz', require: 'xz' }
        }.freeze

        AVAILABLE_METHODS = begin
          available = STDLIB_METHODS.dup

          GEM_REQUIREMENTS.each do |method, gem_info|
            begin
              require gem_info[:require]
              available << method
            rescue LoadError
              # Gem not available, skip
            end
          end

          Set.new(available).freeze
        end

        class << self
          def validated_method?(method)
            method if AVAILABLE_METHODS.include?(method)
          end
        
          def compress(data, method)
            case validated_method?(method)
            when 'gzip'
              gzip_compress(data)
            when 'deflate'
              deflate_compress(data)
            when 'br'
              brotli_compress(data)
            when 'zstd'
              zstd_compress(data)
            when 'lz4'
              lz4_compress(data)
            when 'bz2'
              bz2_compress(data)
            when 'xz'
              xz_compress(data)
            else
              data
            end
          end

          def decompress(data, method)
            # Net::HTTP (the default HTTP library) automatically decompresses gzip and deflate. 
            # However, we support custom setups via the `connection` setting. We'll check to see
            # if the data has already been decompressed first
            case validated_method?(method)
            when 'gzip'
              data.start_with?("\x1f\x8b".b) ? gzip_decompress(data) : data
            when 'deflate'
              data.start_with?("\x78".b) ? deflate_decompress(data) : data
            when 'br'
              brotli_decompress(data)
            when 'zstd'
              zstd_decompress(data)
            when 'lz4'
              lz4_decompress(data)
            when 'bz2'
              bz2_decompress(data)
            when 'xz'
              xz_decompress(data)
            else
              data
            end
          end

          private

          def gzip_compress(data)
            output = StringIO.new
            output.set_encoding('BINARY')
            gz = Zlib::GzipWriter.new(output)
            gz.write(data)
            gz.close
            output.string
          end

          def gzip_decompress(data)
            gz = Zlib::GzipReader.new(StringIO.new(data))
            gz.read
          ensure
            gz&.close
          end

          def deflate_compress(data)
            Zlib::Deflate.deflate(data)
          end

          def deflate_decompress(data)
            Zlib::Inflate.inflate(data)
          end

          def brotli_compress(data)
            Brotli.deflate(data)
          end

          def brotli_decompress(data)
            Brotli.inflate(data)
          end

          def zstd_compress(data)
            Zstd.compress(data)
          end

          def zstd_decompress(data)
            Zstd.decompress(data)
          end

          def lz4_compress(data)
            LZ4.compress(data)
          end

          def lz4_decompress(data)
            LZ4.uncompress(data)
          end

          def bz2_compress(data)
            output = StringIO.new
            output.set_encoding('BINARY')
            Bzip2::FFI::Writer.open(output) do |writer|
              writer.write(data)
            end
            output.string
          end

          def bz2_decompress(data)
            Bzip2::FFI::Reader.read(StringIO.new(data))
          end

          def xz_compress(data)
            XZ.compress(data)
          end

          def xz_decompress(data)
            XZ.decompress(data)
          end
        end
      end
    end
  end
end
