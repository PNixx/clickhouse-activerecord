# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition

        attr_reader :view, :materialized, :if_not_exists

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
        end

        def integer(*args, **options)
          if options[:limit] == 8
            args.each { |name| column(name, :big_integer, options.except(:limit)) }
          else
            super
          end
        end

      end
    end
  end
end
