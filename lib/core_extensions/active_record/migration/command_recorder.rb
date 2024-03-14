module CoreExtensions
  module ActiveRecord
    module Migration
      module CommandRecorder
        def create_view(*args, &block)
          record(:create_view, args, &block)
        end

        private

        def invert_create_view(args)
          view_name, options = args
          [:drop_table, view_name, options]
        end
      end
    end
  end
end
