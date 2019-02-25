# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition

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
