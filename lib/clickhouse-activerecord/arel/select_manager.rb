module ClickhouseActiverecord::Arel

    class SelectManager < ::Arel::SelectManager

      def using(*exprs)
        @ctx.source.right.last.right = ClichouseActiverecord::Arel::Nodes::Using.new(::Arel.sql(exprs.join(',')))
        self
      end

    end

end