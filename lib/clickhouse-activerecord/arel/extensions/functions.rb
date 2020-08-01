module ClickhouseActiverecord::Arel::Extensions

  module Functions

    ONE_ARGUMENT = [ 'any','anyHeavy','sumWithOverflow','anyLast',
                     'groupBitAnd','groupBitOr','groupBitXor', 'groupBitmap',
                     'groupBitmapAnd','groupBitmapOr','groupBitmapXor',
                     'skewPop','skewSamp','kurtPop','kurtSamp',
                     'unique','uniqueExact','median','medianExact','varSamp','varPop',
                     'stddevSamp','stddevPop'
                    ]

    STANDARD_ONE_ARGUMENT = ['minimum','maximum','average','sum']

    TWO_ARGUMENT = ['argMin','argMax','sumMap']

    FUNCTION_ALIASES = {
        'unique' => 'uniq',
        'uniqueExact' => 'uniqExact',
        'minimum' => 'min',
        'maximum' => 'max',
        'average' => 'avg'
    }

    ONE_ARGUMENT.each do |funcname|
      define_method funcname do
        ClickhouseActiverecord::Arel::Nodes::FunctionZero.new [self], (FUNCTION_ALIASES[funcname] || funcname)
      end
    end

    TWO_ARGUMENT.each do |funcname|
      define_method funcname do | argument |
        ClickhouseActiverecord::Arel::Nodes::FunctionOne.new [self], (FUNCTION_ALIASES[funcname] || funcname),
                                                             argument
      end
    end


    ## Conditional aggregate functions

    def countIf(condition)
      ClickhouseActiverecord::Arel::Nodes::FunctionCountIf.new [self], 'countIf',
                                                               condition
    end

    (ONE_ARGUMENT + STANDARD_ONE_ARGUMENT).each do |funcname|
      define_method funcname + 'If' do | condition |
        ClickhouseActiverecord::Arel::Nodes::FunctionOne.new [self], (FUNCTION_ALIASES[funcname] || funcname)+'If',
                                                             condition
      end
    end

    TWO_ARGUMENT.each do |funcname|
      define_method funcname + 'If' do | argument, condition |
        ClickhouseActiverecord::Arel::Nodes::FunctionTwo.new [self],
                                                             (FUNCTION_ALIASES[funcname] || funcname)+'If',
                                                             argument,
                                                             condition
      end
    end

  end
end