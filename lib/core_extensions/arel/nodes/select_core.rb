module CoreExtensions
  module Arel # :nodoc: all
    module Nodes
      module SelectCore
        attr_accessor :final

        def source
          return super unless final

          ::Arel::Nodes::Final.new(super)
        end

        def hash
          [
            @source, @set_quantifier, @projections, @optimizer_hints,
            @wheres, @groups, @havings, @windows, @comment, @final
          ].hash
        end

        def eql?(other)
          super && final == other.final
        end
      end
    end
  end
end
