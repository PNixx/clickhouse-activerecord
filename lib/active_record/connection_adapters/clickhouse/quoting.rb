module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module Quoting
        extend ActiveSupport::Concern

        module ClassMethods # :nodoc:
          def quote_column_name(name)
            name
          end

          def quote_table_name(name)
            name
          end
        end
      end
    end
  end
end
