module ClickhouseActiverecord::Arel

  class SelectStatement < ::Arel::Nodes::SelectStatement
    attr_accessor :limit_by

    def eql? other
      super(other) && self.limit_by==other.limit.by
    end

  end

end