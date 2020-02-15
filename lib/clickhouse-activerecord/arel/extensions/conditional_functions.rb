module ClickhouseActiverecord::Arel::Extensions

  module ConditionalFunctions

    def countIf condition
      ClickhouseActiverecord::Arel::Nodes::CountIf.new [self], condition
    end

    def sumIf condition
      ClickhouseActiverecord::Arel::Nodes::SumIf.new [self], condition
    end

  end
end