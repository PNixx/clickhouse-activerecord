module CoreExtensions
  module Arel # :nodoc: all
    module Nodes
      module SelectCore
        attr_accessor :final, :prewheres

        def initialize(relation = nil)
          super
          @prewheres = []
        end

        def initialize_copy(other)
          super
          @prewheres = @prewheres.clone
        end

        def source
          return super unless final

          ::Arel::Nodes::Final.new(super)
        end

        def hash
          [
            @source, @set_quantifier, @projections, @optimizer_hints,
            @prewheres, @wheres, @groups, @havings, @windows, @comment, @final
          ].hash
        end

        def eql?(other)
          super && final == other.final && prewheres == other.prewheres
        end
      end
    end
  end
end
