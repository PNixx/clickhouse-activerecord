# frozen_string_literal: true

RSpec.describe 'Model', :migrations do
  let(:model) do
    Class.new(ActiveRecord::Base) do
      self.table_name = 'events'
    end
  end

  before do
    migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
    quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
  end

  let(:date) { Date.today }

  describe '#create' do
    it 'creates a new record' do
      expect {
        model.create!(
          event_name: 'some event',
          date: date
        )
      }.to change { model.count }
    end
  end

  describe '#update' do
    let(:record) { model.create!(event_name: 'some event', date: date) }

    it 'raises an error' do
      expect {
        record.update!(event_name: 'new event name')
      }.to raise_error(ActiveRecord::ActiveRecordError, 'Clickhouse update is not supported')
    end
  end

  describe '#destroy' do
    let(:record) { model.create!(event_name: 'some event', date: date) }

    it 'raises an error' do
      expect {
        record.destroy!
      }.to raise_error(ActiveRecord::ActiveRecordError, 'Clickhouse delete is not supported')
    end
  end
end
