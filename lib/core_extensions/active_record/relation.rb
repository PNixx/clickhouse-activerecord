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

      def settings!(**opts) # :nodoc:
        raise ::ActiveRecord::ActiveRecordError, 'SETTINGS is a ClickHouse-specific query clause' unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

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

      private

      def build_arel(aliases = nil)
        arel = super

        arel.settings(settings_values) unless settings_values.empty?

        arel
      end

    end
  end
end
