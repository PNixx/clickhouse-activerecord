# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition

        attr_reader :view, :materialized, :if_not_exists, :to

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
          @to = to
        end

        def integer(*args, **options)
          kind = @conn.send(:resolve_integer_kind, options)
          args.each { |name| column(name, kind, **options.except(:limit, :unsigned)) }
        end

        def datetime(*args, **options)
          kind = :datetime

          if options[:precision]
            kind = :datetime64
          end

          args.each { |name| column(name, kind, **options) }
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

        def column(name, type, index: nil, **options)
          options[:null] = false if type.match?(/Nullable\([^)]+\)/)
          super(name, type, index: index, **options)
        end

        private

        def valid_column_definition_options
          super + [:array, :low_cardinality, :fixed_string, :value, :type, :map, :codec, :unsigned]
        end
      end

      class IndexDefinition
        attr_reader :table, :name, :expression, :type, :granularity, :first, :after, :if_exists, :if_not_exists

        def initialize(table, name, expression, type, granularity, first:, after:, if_exists:, if_not_exists:)
          @table = table
          @name = name
          @expression = expression
          @type = type
          @granularity = granularity
          @first = first
          @after = after
          @if_exists = if_exists
          @if_not_exists = if_not_exists
        end

      end
    end
  end
end
