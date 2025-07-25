require 'arel/visitors/to_sql'

module Arel
  module Visitors
    class Clickhouse < ::Arel::Visitors::ToSql

      def aggregate(name, o, collector)
        # replacing function name for materialized view
        if o.expressions.first && o.expressions.first != '*' && !o.expressions.first.is_a?(String) && o.expressions.first.relation&.is_view
          super("#{name.downcase}Merge", o, collector)
        else
          super
        end
      end

      # https://clickhouse.com/docs/en/sql-reference/statements/delete
      # DELETE and UPDATE in ClickHouse working only without table name
      def visit_Arel_Attributes_Attribute(o, collector)
        collector << quote_table_name(o.relation.table_alias || o.relation.name) << '.' unless collector.value.start_with?('DELETE FROM ') || collector.value.include?(' UPDATE ')
        collector << quote_column_name(o.name)
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

      def visit_Arel_Nodes_Final(o, collector)
        visit o.expr, collector
        collector << ' FINAL'
        collector
      end

      def visit_Arel_Nodes_Settings(o, collector)
        return collector if o.expr.empty?

        collector << "SETTINGS "
        o.expr.each_with_index do |(key, value), i|
          collector << ", " if i > 0
          collector << key.to_s.gsub(/\W+/, "")
          collector << " = "
          collector << sanitize_as_setting_value(value)
        end
        collector
      end

      def visit_Arel_Nodes_Using o, collector
        collector << "USING "
        visit o.expr, collector
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
