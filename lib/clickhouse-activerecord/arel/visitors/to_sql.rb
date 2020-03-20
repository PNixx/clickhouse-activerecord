require 'arel/visitors/to_sql'

module ClickhouseActiverecord
  module Arel
    module Visitors
      class ToSql < ::Arel::Visitors::ToSql

        def visit_ClickhouseActiverecord_Arel_Nodes_Using o, collector
          collector << " USING "
          visit o.expr, collector
          collector
        end

        def visit_ClickhouseActiverecord_Arel_Nodes_FunctionZero o, collector
          collector << "#{o.funcname}("
          collector = inject_join(o.expressions, collector, ", ")
          collector << ")"
          if o.alias
            collector << " AS "
            visit o.alias, collector
          else
            collector
          end
        end

        def visit_ClickhouseActiverecord_Arel_Nodes_FunctionOne o, collector
            collector << "#{o.funcname}("
            collector = inject_join(o.expressions, collector, ", ")  << ","
            visit o.argument, collector
            collector << ")"
            if o.alias
              collector << " AS "
              visit o.alias, collector
            else
              collector
            end
        end

        def visit_ClickhouseActiverecord_Arel_Nodes_FunctionTwo o, collector
          collector << "#{o.funcname}("
          collector = inject_join(o.expressions, collector, ", ")  << ","
          visit o.argument1, collector
          collector << ','
          visit o.argument2, collector
          collector << ")"
          if o.alias
            collector << " AS "
            visit o.alias, collector
          else
            collector
          end
        end

        def visit_Arel_Nodes_Count o, collector
          if o.expressions==[::Arel.star]
            collector << "count()"
            if o.alias
              collector << " AS "
              visit o.alias, collector
            else
              collector
            end
          else
            super o, collector
          end
        end

        def visit_ClickhouseActiverecord_Arel_Nodes_FunctionCountIf o, collector
          collector << "countIf("
          unless o.expressions==[::Arel.star]
            collector = inject_join(o.expressions, collector, ", ") << ","
          end
          visit o.argument, collector
          collector << ")"
          if o.alias
            collector << " AS "
            visit o.alias, collector
          else
            collector
          end
        end

        def visit_ClickhouseActiverecord_Arel_Nodes_To o, collector
          collector << "to#{o.type}("
          visit o.expressions, collector
          collector << ')'
          collector
        end

      end
    end
  end
end
