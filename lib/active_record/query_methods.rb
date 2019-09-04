# frozen_string_literal: true

module ActiveRecord
  module QueryMethods
    # Replace for only ClickhouseAdapter
    def reverse_order!
      orders = order_values.uniq
      orders.reject!(&:blank?)
      if self.connection.is_a?(ConnectionAdapters::ClickhouseAdapter) && orders.empty?
        self.order_values = %w(date created_at).select {|c| column_names.include?(c) }.map{|c| arel_attribute(c).desc }
      else
        self.order_values = reverse_sql_order(orders)
      end
      self
    end
  end
end
