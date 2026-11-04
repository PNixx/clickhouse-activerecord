module Arel # :nodoc: all
  module Nodes
    # Wraps the table source of a JOIN so the ClickHouse visitor renders
    # `<table> FINAL ON ...` for that join. Built by
    # ActiveRecord::Relation#joins_final, which marks the matching join
    # sources after Arel has been constructed.
    class FinalTable < Arel::Nodes::Unary
    end
  end
end
