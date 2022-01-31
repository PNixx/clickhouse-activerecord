# frozen_string_literal: true

module ClickhouseActiverecord
  module Relation
    def reverse_order!
      return super unless connection.is_a?(ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

      orders = order_values.uniq.compact_blank
      return super unless orders.empty? && !primary_key

      self.order_values = (column_names & %w[date created_at]).map { |c| arel_table[c].desc }
      self
    end
  end
end
