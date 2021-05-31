module Util
  module Statement
    END_OF_STATEMENT = ';'
    END_OF_STATEMENT_RE = /#{END_OF_STATEMENT}(\s+|\Z)/.freeze

    module_function

    def ensure(truthful, value, fallback = nil)
      truthful ? value : fallback
    end

    def format(sql, format)
      return sql if sql.match?(/FORMAT/i)

      "#{sql.sub(END_OF_STATEMENT_RE, '')} FORMAT #{format};"
    end
  end
end