# frozen_string_literal: true

module CoreExtensions
  module ActiveRecord
    module Relation

      def reverse_order!
        return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

        orders = order_values.uniq.compact_blank
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
      def settings!(**opts) # :nodoc:
        check_command!('SETTINGS')
        self.settings_values = settings_values.merge opts
        self
      end

      def settings_values
        @values.fetch(:settings, ::ActiveRecord::QueryMethods::FROZEN_EMPTY_HASH)
      end

      def settings_values=(value)
        assert_mutability!
        @values[:settings] = value
      end

      # When FINAL is specified, ClickHouse fully merges the data before returning the result and thus performs all data transformations that happen during merges for the given table engine.
      # For example:
      #
      #   users = User.final.all
      #   # SELECT users.* FROM users FINAL
      #
      # An <tt>ActiveRecord::ActiveRecordError</tt> will be raised if database not ClickHouse.
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
        assert_mutability!
        @values[:final] = value
      end

      def final_value
        @values.fetch(:final, nil)
      end

      private

      def check_command!(cmd)
        raise ::ActiveRecord::ActiveRecordError, "#{cmd} is a ClickHouse specific query clause" unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
      end

      def build_arel(aliases = nil)
        arel = super

        arel.final! if final_value
        arel.settings(settings_values) unless settings_values.empty?

        arel
      end

    end
  end
end
