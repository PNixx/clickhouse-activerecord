module ClickhouseActiverecord::Arel

    class SelectManager < ::Arel::SelectManager

      def using(*exprs)
        @ctx.source.right.last.right = ::ClickhouseActiverecord::Arel::Nodes::Using.new(::Arel.sql(exprs.join(',')))
        self
      end

      ::Arel::Nodes::SqlLiteral.class_eval do
        include ::ClickhouseActiverecord::Arel::Extensions::ConditionalFunctions
      end

      ::Arel::Nodes::InfixOperation.class_eval do
        include ::ClickhouseActiverecord::Arel::Extensions::ConditionalFunctions
      end

      ::Arel::Attributes::Attribute.class_eval do
        include ::ClickhouseActiverecord::Arel::Extensions::ConditionalFunctions
      end

    end

end