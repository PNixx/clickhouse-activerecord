# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition

        attr_reader :view, :materialized, :dictionary, :if_not_exists, :to

        def initialize(
            conn,
            name,
            temporary: false,
            if_not_exists: false,
            options: nil,
            as: nil,
            comment: nil,
            view: false,
            materialized: false,
            dictionary: false,
            to: nil,
            **
          )
          @conn = conn
          @columns_hash = {}
          @indexes = []
          @foreign_keys = []
          @primary_keys = nil
          @temporary = temporary
          @if_not_exists = if_not_exists
          @options = options
          @as = as
          @name = name
          @comment = comment
          @view = view || materialized
          @materialized = materialized
          @dictionary = dictionary
          @to = to
        end

        def integer(*args, **options)
          # default to unsigned
          unsigned = options[:unsigned]
          unsigned = true if unsigned.nil?

          kind = :uint32 # default

          if options[:limit]
            if unsigned
              kind = :uint8       if options[:limit] == 1
              kind = :uint16      if options[:limit] == 2
              kind = :uint32      if [3,4].include?(options[:limit])
              kind = :uint64      if [5,6,7].include?(options[:limit])
              kind = :big_integer if options[:limit] == 8
              kind = :uint256     if options[:limit] > 8
            else
              kind = :int8       if options[:limit] == 1
              kind = :int16      if options[:limit] == 2
              kind = :int32      if [3,4].include?(options[:limit])
              kind = :int64     if options[:limit] > 5 && options[:limit] <= 8
              kind = :int128     if options[:limit] > 8 && options[:limit] <= 16
              kind = :int256     if options[:limit] > 16
            end
          end
          args.each { |name| column(name, kind, **options.except(:limit, :unsigned)) }
        end

        def datetime(*args, **options)
          kind = :datetime

          if options[:precision]
            kind = :datetime64
            options[:value] = options[:precision]
          end

          args.each { |name| column(name, kind, **options.except(:precision)) }
        end

        def uuid(*args, **options)
          args.each { |name| column(name, :uuid, **options) }
        end

        def enum(*args, **options)
          kind = :enum8

          unless options[:value].is_a? Hash
            raise ArgumentError, "Column #{args.first}: option 'value' must be Hash, got: #{options[:value].class}"
          end

          options[:value] = options[:value].each_with_object([]) { |(k, v), arr| arr.push("'#{k}' = #{v}") }.join(', ')

          if options[:limit]
            kind = :enum8  if options[:limit] == 1
            kind = :enum16 if options[:limit] == 2
          end

          args.each { |name| column(name, kind, **options.except(:limit)) }
        end
      end
    end
  end
end
