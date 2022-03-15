# frozen_string_literal: true

RSpec.describe 'TypeMap', :migrations do

  let(:model) do
    Class.new(ActiveRecord::Base) do
      self.table_name = 'some'
    end
  end

  before do
    migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'plain_table_creation')
    quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
  end

  describe 'initialization' do
    subject { model.connection.send(:type_map) }

    it 'registers Clickhouse types' do
      expect(subject.lookup(/Array/)).to be_a ActiveRecord::ConnectionAdapters::Clickhouse::OID::Array
      expect(subject.lookup('Date')).to be_a ActiveRecord::ConnectionAdapters::Clickhouse::OID::Date
    end
  end

end
