require 'arel/visitors/to_sql'

module ClickhouseActiverecord
  module Arel
    module Visitors
      class ToSql < ::Arel::Visitors::ToSql

        def aggregate(name, o, collector)
          # todo how get model class from request? This method works with only rails 4.2.
          # replacing function name for materialized view
          # if o.expressions.first && o.expressions.first != '*' && !o.expressions.first.is_a?(String) && o.expressions.first.relation && o.expressions.first.relation.engine && o.expressions.first.relation.engine.is_view
          #   super("#{name.downcase}Merge", o, collector)
          # else
            super
          # end
        end

      end
    end
  end
end
