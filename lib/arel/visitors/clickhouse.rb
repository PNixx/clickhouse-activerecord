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

      def visit_Arel_Nodes_UpdateStatement(o, collector)
        o = prepare_update_statement(o)

        collector << 'ALTER TABLE '
        collector = visit o.relation, collector
        collect_nodes_for o.values, collector, ' UPDATE '
        collect_nodes_for o.wheres, collector, ' WHERE ', ' AND '
        collect_nodes_for o.orders, collector, ' ORDER BY '
        maybe_visit o.limit, collector
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

      def visit_Arel_Nodes_Matches(o, collector)
        op = o.case_sensitive ? " LIKE " : " ILIKE "
        infix_value o, collector, op
      end

      def visit_Arel_Nodes_DoesNotMatch(o, collector)
        op = o.case_sensitive ? " NOT LIKE " : " NOT ILIKE "
        infix_value o, collector, op
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

    end
  end
end
