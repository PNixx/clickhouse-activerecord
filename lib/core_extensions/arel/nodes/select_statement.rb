module CoreExtensions
  module Arel # :nodoc: all
    module Nodes
      module SelectStatement
        attr_accessor :final, :settings

        def initialize
          super
          @final = nil
          @settings = nil
        end

        def eql?(other)
          super && final == other.final && settings == other.settings
        end
      end
    end
  end
end
