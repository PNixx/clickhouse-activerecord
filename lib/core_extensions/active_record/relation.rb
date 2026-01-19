module CoreExtensions
  module ActiveRecord
    module Relation

      def self.prepended(base)
        base::VALID_UNSCOPING_VALUES << :final << :settings
      end

      # Define settings in the SETTINGS clause of the SELECT query. The setting value is applied only to that query and is reset to the default or previous value after the query is executed.
      # For example:
      #
      #   users = User.settings(optimize_read_in_order: 1, cast_keep_nullable: 1).where(name: 'John')
      #   # SELECT users.* FROM users WHERE users.name = 'John' SETTINGS optimize_read_in_order = 1, cast_keep_nullable = 1
      #
      # An <tt>ActiveRecord::ActiveRecordError</tt> will be raised if database not ClickHouse.
      # @param [Hash] opts


      # Specify settings to be used for this single query.
      # For example:
      #
      #   users = User.settings(use_skip_indexes: true).where(name: 'John')
      #   # SELECT "users".* FROM "users"
      #   # WHERE "users"."name" = 'John'
      #   # SETTINGS use_skip_indexes = 1
      def settings(**opts)
        spawn.settings!(**opts)
      end

      # @param [Hash] opts
      def settings!(**opts)
        check_command!('SETTINGS')
        self.settings_values = settings_values.merge opts
        self
      end

      def settings_values
        @values.fetch(:settings, ::ActiveRecord::QueryMethods::FROZEN_EMPTY_HASH)
      end

      def settings_values=(value)
        if ::ActiveRecord::version >= Gem::Version.new('7.2')
          assert_modifiable!
        else
          assert_mutability!
        end
        @values[:settings] = value
      end

      # When FINAL is specified, ClickHouse fully merges the data before returning the result and thus performs all data transformations that happen during merges for the given table engine.
      # For example:
      #
      #   users = User.final.all
      #   # SELECT users.* FROM users FINAL
      #
      # An <tt>ActiveRecord::ActiveRecordError</tt> will be raised if database not ClickHouse.
      #
      # @param [Boolean] final
      def final(final = true)
        spawn.final!(final)
      end

      # @param [Boolean] final
      def final!(final = true)
        check_command!('FINAL')
        self.final_value = final
        self
      end

      def final_value=(value)
        if ::ActiveRecord::version >= Gem::Version.new('7.2')
          assert_modifiable!
        else
          assert_mutability!
        end
        @values[:final] = value
      end

      def final_value
        @values.fetch(:final, nil)
      end

      # GROUPING SETS allows you to specify multiple groupings in the GROUP BY clause.
      # Whereas GROUP BY CUBE generates all possible groupings, GROUP BY GROUPING SETS generates only the specified groupings.
      # For example:
      #
      #   users = User.group_by_grouping_sets([], [:name], [:name, :age]).select(:name, :age, 'count(*)')
      #   # SELECT name, age, count(*) FROM users GROUP BY GROUPING SETS ( (), (name), (name, age) )
      #
      # which is generally equivalent to:
      #   # SELECT NULL, NULL, count(*) FROM users
      #   # UNION ALL
      #   # SELECT name, NULL, count(*) FROM users GROUP BY name
      #   # UNION ALL
      #   # SELECT name, age, count(*) FROM users GROUP BY name, age
      #
      # Raises <tt>ArgumentError</tt> if no grouping sets are specified are provided.
      def group_by_grouping_sets(*grouping_sets)
        raise ArgumentError, 'The method .group_by_grouping_sets() must contain arguments.' if grouping_sets.blank?

        spawn.group_by_grouping_sets!(*grouping_sets)
      end

      def group_by_grouping_sets!(*grouping_sets) # :nodoc:
        grouping_sets = grouping_sets.map { |set| arel_columns(set) }
        self.group_values += [::Arel::Nodes::GroupingSets.new(grouping_sets)]
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

      def check_command!(cmd)
        raise ::ActiveRecord::ActiveRecordError, cmd + ' is a ClickHouse specific query clause' unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
      end

      def build_arel(connection_or_aliases = nil, aliases = nil)
        requirement = Gem::Requirement.new('>= 7.2', '< 8.1')

        if requirement.satisfied_by?(::ActiveRecord::version)
          arel = super
        else
          arel = super(connection_or_aliases)
        end

        arel.final! if final_value
        arel.limit_by(*@values[:limit_by]) if @values[:limit_by].present?
        arel.settings(settings_values) unless settings_values.empty?
        arel.using(@values[:using]) if @values[:using].present?
        arel.windows(@values[:windows]) if @values[:windows].present?

        arel
      end
    end
  end
end
