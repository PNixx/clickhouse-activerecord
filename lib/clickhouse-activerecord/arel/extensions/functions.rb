module ClickhouseActiverecord::Arel::Extensions

  module Functions

    ['countIf'].each do |funcname|
      define_method funcname do | condition |
        ClickhouseActiverecord::Arel::Nodes::FunctionZero.new [self], funcname, condition
      end
    end

    ['sumIf','anyIf','anyHeavyIf'].each do |funcname|
      define_method funcname do | condition |
        ClickhouseActiverecord::Arel::Nodes::FunctionOne.new [self], funcname, condition
      end
    end

  end
end