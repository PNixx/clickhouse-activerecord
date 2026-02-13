module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module Quoting
        extend ActiveSupport::Concern

        QUOTED_COLUMN_NAMES = Concurrent::Map.new
        QUOTED_TABLE_NAMES = Concurrent::Map.new

        module ClassMethods # :nodoc:
          def quote_column_name(name)
            QUOTED_COLUMN_NAMES[name] ||= "`#{name.to_s.gsub('`', '``')}`".freeze
          end

          def quote_table_name(name)
            QUOTED_TABLE_NAMES[name] ||= "`#{name.to_s.gsub('`', '``').gsub('.', '`.`')}`".freeze
          end
        end
      end
    end
  end
end
