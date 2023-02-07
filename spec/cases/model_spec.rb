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

  describe '::settings' do
    it 'does not change settings_values when empty' do
      query = model.all
      query = query.settings
      expect(query.settings_values).to be_empty
    end

    it 'updates settings_values' do
      query = model.all
      query = query.settings(foo: 'bar', abc: 123)
      expect(query.settings_values).to eq({ foo: 'bar', abc: 123 })
    end

    it 'overwrites settings_values previously set' do
      query = model.all
      query = query.settings(foo: 'bar', abc: 123)
      query = query.settings(foo: 'baz')
      expect(query.settings_values).to eq({ foo: 'baz', abc: 123 })
    end

    it 'works as a chainable method' do
      query = model.all
      query = query.settings(foo: 'bar', abc: 123).settings(foo: 'baz')
      expect(query.settings_values).to eq({ foo: 'baz', abc: 123 })
    end
  end
end
