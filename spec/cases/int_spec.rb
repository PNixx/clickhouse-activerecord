# frozen_string_literal: true

RSpec.describe 'ints', :migrations do

  let(:model) do
    Class.new(ActiveRecord::Base) do
      self.table_name = 'some'
    end
  end

  before do
    migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_int_creation')
    quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
  end


  describe 'ints' do
    it 'returns all the values as ruby ints' do
      expect(model.create!(
        id: 22,
        col8: 22,
        col16: 22,
        col32: 22,
        col64: 22,
        col128: 22,
        col256: 22
      ).reload.attributes.values).to all eq(22)

    end
  end

end
