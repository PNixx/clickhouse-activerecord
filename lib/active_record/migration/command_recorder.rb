require 'active_record/migration/command_recorder'

module ActiveRecord
  class Migration
    class CommandRecorder
      def create_table_with_distributed(*args, &block)
        record(:create_table_with_distributed, args, &block)
      end

      private

      def invert_create_table_with_distributed(args)
        table_name, options = args
        [:drop_table_with_distributed, table_name, options]
      end
    end
  end
end
