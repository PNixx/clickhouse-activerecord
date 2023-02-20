# frozen_string_literal: true

RSpec.describe 'Model', :migrations do

  let(:date) { Date.today }

  context 'sample' do
    let!(:model) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'events'
      end
    end

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
      quietly { ClickhouseActiverecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
    end


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

  context 'array' do

    let!(:model) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'actions'
      end
    end

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_array_datetime')
      quietly { ClickhouseActiverecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
    end

    describe '#create' do
      it 'creates a new record' do
        expect {
          model.create!(
            array_datetime: [1.day.ago, Time.now, '2022-12-06 15:22:49'],
            array_string: %w[asdf jkl],
            date: date
          )
        }.to change { model.count }
        event = model.first
        expect(event.array_datetime.is_a?(Array)).to be_truthy
        expect(event.array_datetime[0].is_a?(DateTime)).to be_truthy
        expect(event.array_string[0].is_a?(String)).to be_truthy
        expect(event.array_string).to eq(%w[asdf jkl])
      end

      it 'get record' do
        model.connection.insert("INSERT INTO #{model.table_name} (id, array_datetime, date) VALUES (1, '[''2022-12-06 15:22:49'',''2022-12-05 15:22:49'']', '2022-12-06')")
        expect(model.count).to eq(1)
        event = model.first
        expect(event.date.is_a?(Date)).to be_truthy
        expect(event.date).to eq(Date.parse('2022-12-06'))
        expect(event.array_datetime.is_a?(Array)).to be_truthy
        expect(event.array_datetime[0].is_a?(DateTime)).to be_truthy
        expect(event.array_datetime[0]).to eq('2022-12-06 15:22:49')
        expect(event.array_datetime[1]).to eq('2022-12-05 15:22:49')
      end
    end
  end
end
