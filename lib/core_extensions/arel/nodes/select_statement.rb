# frozen_string_literal: true

module CoreExtensions
  module Arel # :nodoc: all
    module Nodes
      module SelectStatement
        attr_accessor :settings

        def initialize(relation = nil)
          super
          @settings = nil
        end

        def hash
          [@cores, @orders, @limit, @lock, @offset, @with, @settings].hash
        end

        def eql?(other)
          super && settings == other.settings
        end
      end
    end
  end
end
