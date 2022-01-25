# frozen_string_literal: true

require 'arel/visitors/clickhouse'

module Arel # :nodoc: all
  module Visitors
    class Clickhouse < Arel::Visitors::ToSql

      def aggregate(name, o, collector)
        if o.expressions.first && o.expressions.first != '*' && !o.expressions.first.is_a?(String) && o.expressions.first.respond_to?(:relation) && o.expressions.first.relation&.is_view
          super("#{name.downcase}Merge", o, collector)
        else
          super
        end
      end

    end
  end
end
