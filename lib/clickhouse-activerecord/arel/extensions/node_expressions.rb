module ClickhouseActiverecord::Arel::Extensions

  module NodeExpressions

    def to type
      ClickhouseActiverecord::Arel::Nodes::To.new self, type
    end

  end
end