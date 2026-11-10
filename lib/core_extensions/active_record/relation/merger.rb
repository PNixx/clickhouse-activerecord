module CoreExtensions
  module ActiveRecord
    module Relation
      # Carry PREWHERE conditions through Relation#merge (and therefore through
      # scopes and associations merged into a query), using the same merge
      # rules ActiveRecord applies to WHERE.
      module Merger
        def merge
          super.tap do
            unless other.prewhere_clause.empty?
              relation.prewhere_clause = relation.prewhere_clause.merge(other.prewhere_clause)
            end
          end
        end
      end
    end
  end
end
