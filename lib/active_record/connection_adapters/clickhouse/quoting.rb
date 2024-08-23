module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module Quoting
        extend ActiveSupport::Concern

        module ClassMethods # :nodoc:
          def quote_column_name(name)
            name.to_s.include?('.') ? "`#{name}`" : name.to_s
          end

          def quote_table_name(name)
            name.to_s
          end
        end
      end
    end
  end
end
