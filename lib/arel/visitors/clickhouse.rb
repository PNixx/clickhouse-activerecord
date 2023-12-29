# frozen_string_literal: true

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

      def visit_Arel_Nodes_Final(o, collector)
        visit o.expr, collector
        collector << ' FINAL'
        collector
      end

      def visit_Arel_Nodes_SelectOptions(o, collector)
        maybe_visit o.settings, super
      end

      def visit_Arel_Nodes_Settings(o, collector)
        return collector if o.expr.empty?

        collector << "SETTINGS "
        o.expr.each_with_index do |(key, value), i|
          collector << ", " if i > 0
          collector << sanitize_as_setting_name(key)
          collector << " = "
          collector << sanitize_as_setting_value(value)
        end
        collector
      end

      def sanitize_as_setting_value(value)
        if value == :default
          'DEFAULT'
        else
          quote(value)
        end
      end

      def sanitize_as_setting_name(value)
        return value if Arel::Nodes::SqlLiteral === value
        @connection.sanitize_as_setting_name(value)
      end

      def visit_Arel_Nodes_Using(o, collector)
        collector << 'USING '
        o.expr.each_with_index do |expr, i|
          collector << ", " if i > 0
          collector << quote_column_name(expr)
        end
        collector
      end

    end
  end
end
