module CoreExtensions
  module ActiveRecord
    module Relation

      def self.prepended(base)
        base::VALID_UNSCOPING_VALUES << :final << :settings << :joins_final
      end

      def reverse_order!
        conn = respond_to?(:lease_connection) ? lease_connection : connection
        return super unless conn.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

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

      # Apply the FINAL modifier to one or more joined tables. Unlike #final,
      # which only adds FINAL to the query's primary FROM table, #joins_final
      # adds the join(s) and renders FINAL on the joined table so ClickHouse
      # merges those rows before joining.
      # For example:
      #
      #   AppControlEvent.final.joins_final(:espm_binary)
      #   # SELECT ... FROM espm_app_control_events FINAL
      #   #   INNER JOIN espm_binaries FINAL ON ...
      #
      # Each argument is an association name and is added as a join (like
      # #joins); FINAL is then rendered on the joined table. Joins are matched
      # by table name, so if the same table is joined more than once every join
      # of that table receives FINAL. An
      # <tt>ActiveRecord::ActiveRecordError</tt> will be raised if the database
      # is not ClickHouse.
      #
      # @param [Array] args
      def joins_final(*args)
        spawn.joins_final!(*args)
      end

      # @param [Array] args
      def joins_final!(*args)
        check_command!('FINAL')
        joins!(*args)
        self.joins_final_values |= args
        self
      end

      # Like #joins_final, but adds the join(s) as LEFT OUTER JOINs (via
      # #left_outer_joins) and renders FINAL on the joined table. Use this when
      # the joined rows must be preserved even with no match (e.g. an optional
      # catalog lookup) while still merging the joined table with FINAL.
      # For example:
      #
      #   AppControlEvent.left_joins_final(:espm_binary)
      #   # SELECT ... FROM espm_app_control_events
      #   #   LEFT OUTER JOIN espm_binaries FINAL ON ...
      #
      # Each argument is an association name and is added as a left outer join
      # (like #left_outer_joins); FINAL is then rendered on the joined table.
      # Joins are matched by table name, so if the same table is joined more
      # than once every join of that table receives FINAL. An
      # <tt>ActiveRecord::ActiveRecordError</tt> will be raised if the database
      # is not ClickHouse.
      #
      # @param [Array] args
      def left_joins_final(*args)
        spawn.left_joins_final!(*args)
      end

      # @param [Array] args
      def left_joins_final!(*args)
        check_command!('FINAL')
        left_outer_joins!(*args)
        self.joins_final_values |= args
        self
      end

      def joins_final_values
        @values.fetch(:joins_final, ::ActiveRecord::QueryMethods::FROZEN_EMPTY_ARRAY)
      end

      def joins_final_values=(value)
        if ::ActiveRecord::version >= Gem::Version.new('7.2')
          assert_modifiable!
        else
          assert_mutability!
        end
        @values[:joins_final] = value
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

      # Wrap the left (table) side of every join source that matches a
      # requested #joins_final argument in an Arel::Nodes::FinalTable so the
      # ClickHouse visitor renders FINAL on that join. Called before
      # +arel.final!+ so the join source is still the plain JoinSource (and not
      # yet wrapped in the FROM-level Final node).
      def mark_final_joins!(arel)
        names = final_join_table_names
        return if names.empty?

        arel.join_sources.each do |join|
          next unless join.respond_to?(:left) && join.respond_to?(:left=)

          left = join.left
          next if left.is_a?(::Arel::Nodes::FinalTable)
          next unless final_join_match?(left, names)

          join.left = ::Arel::Nodes::FinalTable.new(left)
        end
      end

      # Resolve the #joins_final arguments to the set of table names that
      # should carry FINAL. Association names are mapped to their table;
      # anything else is treated as a literal table name.
      def final_join_table_names
        joins_final_values.flatten.filter_map do |arg|
          case arg
          when Symbol, String
            reflection = klass.reflect_on_association(arg.to_sym)
            reflection ? reflection.table_name : arg.to_s
          end
        end.to_set
      end

      # Does the join's table source match one of the requested FINAL names?
      # Handles both Arel::Table (name) and Arel::Nodes::TableAlias (alias name
      # plus the underlying relation's name).
      def final_join_match?(left, names)
        candidates = []
        candidates << left.name if left.respond_to?(:name)
        candidates << left.table_alias if left.respond_to?(:table_alias) && left.table_alias
        candidates << left.relation.name if left.respond_to?(:relation) && left.relation.respond_to?(:name)
        candidates.any? { |c| names.include?(c.to_s) }
      end

      def build_arel(connection_or_aliases = nil, aliases = nil)
        requirement = Gem::Requirement.new('>= 7.2', '< 8.1')

        if requirement.satisfied_by?(::ActiveRecord::version)
          arel = super
        else
          arel = super(connection_or_aliases)
        end

        mark_final_joins!(arel) if joins_final_values.present?
        arel.final! if final_value
        arel.limit_by(*@values[:limit_by]) if @values[:limit_by].present?
        arel.settings(settings_values) unless settings_values.empty?
        arel.using(@values[:using]) if @values[:using].present?
        arel.windows(@values[:windows]) if @values[:windows].present?

        arel
      end

      def build_with_value_from_hash(hash)
        return super if ::ActiveRecord::version >= Gem::Version.new('7.2')

        # Redefine for ActiveRecord < 7.2
        hash.map do |name, value|
          expression =
            case value
            when ::Arel::Nodes::SqlLiteral then ::Arel::Nodes::Grouping.new(value)
            when ::ActiveRecord::Relation then value.arel
            when ::Arel::SelectManager then value
            when Symbol then value
            else
              raise ArgumentError, "Unsupported argument type: `#{value}` #{value.class}"
            end
          ::Arel::Nodes::TableAlias.new(expression, name)
        end
      end

      def build_with_expression_from_value(value, nested = false)
        case value
        when Symbol then value
        else
          super
        end
      end
    end
  end
end
