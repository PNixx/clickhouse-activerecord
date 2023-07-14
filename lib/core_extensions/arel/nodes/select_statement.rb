# frozen_string_literal: true

module CoreExtensions
  module Arel # :nodoc: all
    module Nodes
      module SelectStatement
        attr_accessor :settings

        def initialize(cores = [::Arel::Nodes::SelectCore.new])
          @settings = nil

          return super if ::ActiveRecord.version < Gem::Version.new('7')

          relation = (cores unless cores.is_a?(Array) && cores.all? { |c| c.is_a?(::Arel::Nodes::SelectCore) })
          super(relation)
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
