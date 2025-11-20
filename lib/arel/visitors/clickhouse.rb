require 'arel/visitors/to_sql'

module Arel
  module Visitors
    class Clickhouse < ::Arel::Visitors::ToSql

      def compile(node, collector = Arel::Collectors::SQLString.new)
        @delete_or_update = false
        super
      end

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
        unless @delete_or_update
          join_name  = o.relation.table_alias || o.relation.name
          collector << quote_table_name(join_name) << '.'
        end
        collector << quote_column_name(o.name)
      end

      def visit_Arel_Nodes_SelectOptions(o, collector)
        maybe_visit o.limit_by, collector
        maybe_visit o.settings, super
      end

      def visit_Arel_Nodes_UpdateStatement(o, collector)
        @delete_or_update = true
        o = prepare_update_statement(o)

        collector << 'ALTER TABLE '
        collector = visit o.relation, collector
        collect_nodes_for o.values, collector, ' UPDATE '
        collect_nodes_for o.wheres, collector, ' WHERE ', ' AND '
        collect_nodes_for o.orders, collector, ' ORDER BY '
        maybe_visit o.limit, collector
      end

      def visit_Arel_Nodes_DeleteStatement(o, collector)
        @delete_or_update = true
        super
      end

      def visit_Arel_Nodes_Final(o, collector)
        visit o.expr.left, collector
        collector << ' FINAL'

        o.expr.right.each do |join|
          collector << ' '
          visit join, collector
        end

        collector
      end

      def visit_Arel_Nodes_GroupingSets(o, collector)
        collector << 'GROUPING SETS '
        grouping_array_or_grouping_element(o.expr, collector)
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

      def visit_Arel_Nodes_Using(o, collector)
        collector << "USING "
        visit o.expr, collector
        collector
      end

      def visit_Arel_Nodes_LimitBy(o, collector)
        collector << "LIMIT #{o.expr} BY #{o.column}"
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

      def visit_Arel_Nodes_Rows(o, collector)
        if o.expr.is_a?(String)
          collector << "ROWS #{o.expr}"
        else
          super
        end
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

      private

      # Utilized by GroupingSet, Cube & RollUp visitors to
      # handle grouping aggregation semantics
      def grouping_array_or_grouping_element(o, collector)
        if o.is_a? Array
          collector << '( '
          o.each_with_index do |el, i|
            collector << ', ' if i > 0
            grouping_array_or_grouping_element el, collector
          end
          collector << ' )'
        elsif o.respond_to? :expr
          visit o.expr, collector
        else
          visit o, collector
        end
      end

    end
  end
end
