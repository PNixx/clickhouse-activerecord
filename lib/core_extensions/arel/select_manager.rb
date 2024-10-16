module CoreExtensions
  module Arel
    module SelectManager

      def final!
        @ctx.final = true
        self
      end

      # @param [Hash] values
      def settings(values)
        @ast.settings = ::Arel::Nodes::Settings.new(values)
        self
      end

      # @param [Array] windows
      def windows(windows)
        @ctx.windows = windows.map do |name, opts|
          # https://github.com/rails/rails/blob/main/activerecord/test/cases/arel/select_manager_test.rb#L790
          window = ::Arel::Nodes::NamedWindow.new(name)
          opts.each do |key, value|
            window.send(key, value)
          end
          window
        end
      end

      def using(*exprs)
        @ctx.source.right.last.right = ::Arel::Nodes::Using.new(::Arel.sql(exprs.join(',')))
        self
      end

      def limit_by(*exprs)
        @ast.limit_by = ::Arel::Nodes::LimitBy.new(*exprs)
        self
      end
    end
  end
end
