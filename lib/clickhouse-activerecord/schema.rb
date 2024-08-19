# frozen_string_literal: true

module ClickhouseActiverecord
  class Schema < ::ActiveRecord::Schema
    def define(...)
      ActiveRecord.deprecator.warn(<<~MSG)
        ClickhouseActiverecord::Schema is deprecated
        and will be removed in 1.2 version. Use ActiveRecord::Schema instead.
      MSG
      super
    end
  end
end
