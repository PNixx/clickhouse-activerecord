module CoreExtensions
  module ActiveRecord
    module InsertAll
      module Builder

        def settings_list
          settings = insert_all.settings_values
          connection.visitor.compile(::Arel::Nodes::Settings.new(settings)) unless settings.empty?
        end
      end
    end
  end
end
