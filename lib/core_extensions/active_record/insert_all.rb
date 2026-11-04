module CoreExtensions
  module ActiveRecord
    module InsertAll

      # The `Relation#settings_values` of the relation that runs the insert.
      # Rails 7.1 builds InsertAll from the model inside the relation's `scoping` block and keeps no
      # relation, so `model.all` gives that relation back, the same way it gives one to Rails 7.2.
      def settings_values
        relation = ::ActiveRecord::version >= Gem::Version.new('7.2') ? @relation : model.all
        relation.settings_values
      end
    end
  end
end
