require 'arel/visitors/to_sql'

module ClickhouseActiverecord
  module Arel
    module Visitors
      class ToSql < ::Arel::Visitors::ToSql

        def aggregate(name, o, collector)
          # replacing function name for materialized view
          if o.expressions.first && o.expressions.first != '*' && !o.expressions.first.is_a?(String) && o.expressions.first.relation&.is_view
            super("#{name.downcase}Merge", o, collector)
          else
            super
          end
        end

        def visit_ClichouseActiverecord_Arel_Nodes_Using o, collector
          collector << " USING "
          visit o.expr, collector
          collector
        end

      end
    end
  end
end
