module CoreExtensions
  module ActiveRecord
    module Relation
      def reverse_order!
        return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

        orders = order_values.uniq.reject(&:blank?)
        return super unless orders.empty? && !primary_key

        self.order_values = (column_names & %w[date created_at]).map { |c| arel_table[c].desc }
        self
      end

      # Define settings in the SETTINGS clause of the SELECT query. The setting value is applied only to that query and is reset to the default or previous value after the query is executed.
      # For example:
      #
      #   users = User.settings(optimize_read_in_order: 1, cast_keep_nullable: 1).where(name: 'John')
      #   # SELECT users.* FROM users WHERE users.name = 'John' SETTINGS optimize_read_in_order = 1, cast_keep_nullable = 1
      #
      # An <tt>ActiveRecord::ActiveRecordError</tt> will be raised if database not ClickHouse.
      # @param [Hash] opts
      def settings(**opts)
        spawn.settings!(**opts)
      end

      # @param [Hash] opts
      def settings!(**opts)
        check_command('SETTINGS')
        @values[:settings] = (@values[:settings] || {}).merge opts
        self
      end

      # When FINAL is specified, ClickHouse fully merges the data before returning the result and thus performs all data transformations that happen during merges for the given table engine.
      # For example:
      #
      #   users = User.final.all
      #   # SELECT users.* FROM users FINAL
      #
      # An <tt>ActiveRecord::ActiveRecordError</tt> will be raised if database not ClickHouse.
      def final
        spawn.final!
      end

      def final!
        check_command('FINAL')
        @values[:final] = true
        self
      end

      # The USING clause specifies one or more columns to join, which establishes the equality of these columns. For example:
      #
      #   users = User.joins(:joins).using(:event_name, :date)
      #   # SELECT users.* FROM users INNER JOIN joins USING event_name,date
      #
      # An <tt>ActiveRecord::ActiveRecordError</tt> will be raised if database not ClickHouse.
      # @param [Array] opts
      def using(*opts)
        spawn.using!(*opts)
      end

      # @param [Array] opts
      def using!(*opts)
        @values[:using] = opts
        self
      end

      # Windows functions let you perform calculations across a set of rows that are related to the current row. For example:
      #
      #   users = User.window('x', order: 'date', partition: 'name', rows: 'UNBOUNDED PRECEDING').select('sum(value) OVER x')
      #   # SELECT sum(value) OVER x FROM users WINDOW x AS (PARTITION BY name ORDER BY date ROWS UNBOUNDED PRECEDING)
      #
      # @param [String] name
      # @param [Hash] opts
      def window(name, **opts)
        spawn.window!(name, **opts)
      end

      def window!(name, **opts)
        @values[:windows] = [] unless @values[:windows]
        @values[:windows] << [name, opts]
        self
      end

      # The LIMIT BY clause permit to improve deduplication based on a unique key, it has better performances than
      # the GROUP BY clause
      #
      #   users = User.limit_by(1, id)
      #   # SELECT users.* FROM users LIMIT 1 BY id
      #
      # An <tt>ActiveRecord::ActiveRecordError</tt> will be reaised if database is not Clickhouse.
      # @param [Array] opts
      def limit_by(*opts)
        spawn.limit_by!(*opts)
      end

      # @param [Array] opts
      def limit_by!(*opts)
        @values[:limit_by] = *opts
        self
      end

      private

      def check_command(cmd)
        raise ::ActiveRecord::ActiveRecordError, cmd + ' is a ClickHouse specific query clause' unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
      end

      def build_arel(connection_or_aliases = nil, aliases = nil)
        if ::ActiveRecord::version >= Gem::Version.new('7.2')
          arel = super
        else
          arel = super(connection_or_aliases)
        end

        arel.final! if @values[:final].present?
        arel.limit_by(*@values[:limit_by]) if @values[:limit_by].present?
        arel.settings(@values[:settings]) if @values[:settings].present?
        arel.using(@values[:using]) if @values[:using].present?
        arel.windows(@values[:windows]) if @values[:windows].present?

        arel
      end
    end
  end
end
