# frozen_string_literal: true

RSpec.describe 'TypeMap', :migrations do

  let(:model) do
    Class.new(ActiveRecord::Base) do
      self.table_name = 'some'
    end
  end

  describe 'initialization' do

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'plain_table_creation')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
    end

    subject { model.connection.send(:type_map) }

    it 'registers Clickhouse types' do
      expect(subject.lookup('Array(UInt8)')).to be_a ActiveRecord::ConnectionAdapters::Clickhouse::OID::Array
      expect(subject.lookup('Date')).to be_a ActiveRecord::ConnectionAdapters::Clickhouse::OID::Date
    end

    it 'extracts limits' do
      expect(subject.lookup('UInt64')).to have_attributes(limit: 8)
    end

  end

  describe 'casting' do

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_int_creation')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
    end

    it 'casts all large integer types to Ruby integers' do
      instance = model.create!(
        id: 22,
        col8: 22,
        col16: 22,
        col32: 22,
        col64: 22,
        col128: 22,
        col256: 22
      )
      instance.reload
      expect(instance.attributes.values).to all eq(22)
    end

  end

end
