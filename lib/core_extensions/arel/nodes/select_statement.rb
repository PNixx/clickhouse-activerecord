module CoreExtensions
  module Arel # :nodoc: all
    module Nodes
      module SelectStatement
        attr_accessor :limit_by, :settings

        def initialize(relation = nil)
          super
          @limit_by = nil
          @settings = nil
        end

        def hash
          [@cores, @orders, @limit, @lock, @offset, @with, @settings].hash
        end

        def eql?(other)
          super && 
            limit_by == other.limit_by &&
            settings == other.settings
        end
      end
    end
  end
end
