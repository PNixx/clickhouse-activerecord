# frozen_string_literal: true

RSpec.describe 'Model', :migrations do
  let(:date) { Date.today }

  context 'sample' do
    let!(:model) do
      class ModelJoin < ActiveRecord::Base
        self.table_name = 'joins'
        belongs_to :model, class_name: 'Model'
      end

      class Model < ActiveRecord::Base
        self.table_name = 'sample'
        has_many :joins, class_name: 'ModelJoin', primary_key: 'event_name'
      end

      Model
    end

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
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

    if ActiveRecord::version >= Gem::Version.new('6.1')
      describe '#insert_all' do
        it 'inserts all records' do
          model.insert_all([
                             { event_name: 'some event 1', date: date },
                             { event_name: 'some event 2', date: date },
                           ])
          expect(model.count).to eq(2)
        end
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

    describe '#reverse_order!' do
      it 'blank' do
        descending = model.order(date: :desc)
        ascending = descending.reverse_order
        expect(ascending.map(&:event_name)).to eq([])
      end

      it 'select' do
        model.create!(event_name: 'older event', date: 2.day.ago)
        model.create!(event_name: 'newer event', date: 1.day.ago)

        descending = model.order(date: :desc)
        ascending = descending.reverse_order
        expect(ascending.map(&:event_name)).to eq(['older event', 'newer event'])
      end
    end

    describe 'convert type with aggregations' do
      it 'returns integers' do
        model.create!(event_name: 'some event', event_value: 1, date: date)
        model.create!(event_name: 'some event', event_value: 3, date: date)

        expect(model.select(Arel.sql('sum(event_value) AS event_value')).to_a.first.event_value).to be_a(Integer)
        expect(model.select(Arel.sql('sum(event_value) AS value')).to_a.first.attributes['value']).to be_a(Integer)
        expect(model.pluck(Arel.sql('sum(event_value)')).to_a.first[0]).to be_a(Integer)
      end
    end

    describe 'boolean column type' do
      it 'returns boolean' do
        model.create!(event_name: 'some event', event_value: 1, date: date)
        expect(model.first.enabled.class).to eq(FalseClass)
      end
    end

    describe '#final' do
      it 'issues a FINAL query' do
        model.create!(date: date, event_name: '1')
        model.create!(date: date, event_name: '1')

        expect(model.count).to eq(2)
        expect(model.final.count).to eq(1)
      end
    end

    xdescribe '#using' do
      it 'works' do
        sql = model.joins(:joins).using(:event_name, :date).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample INNER JOIN joins USING event_name, date')
      end
    end
  end

  context 'DateTime64' do

    let!(:model) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'some'
      end
    end

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_datetime_creation')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
    end

    describe '#create' do
      it 'create a new record' do
        time = DateTime.parse('2023-07-21 08:00:00.123')
        model.create!(datetime: time, datetime64: time)
        row = model.first
        expect(row.datetime).to_not eq(row.datetime64)
        expect(row.datetime.strftime('%Y-%m-%d %H:%M:%S')).to eq('2023-07-21 08:00:00')
        expect(row.datetime64.strftime('%Y-%m-%d %H:%M:%S.%3N')).to eq('2023-07-21 08:00:00.123')
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
      quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
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
        expect(event.array_datetime).to be_a(Array)
        expect(event.array_datetime[0]).to be_a(DateTime)
        expect(event.array_string[0]).to be_a(String)
        expect(event.array_string).to eq(%w[asdf jkl])
      end

      it 'get record' do
        model.connection.insert("INSERT INTO #{model.table_name} (id, array_datetime, date) VALUES (1, '[''2022-12-06 15:22:49'',''2022-12-05 15:22:49'']', '2022-12-06')")
        expect(model.count).to eq(1)
        event = model.first
        expect(event.date).to be_a(Date)
        expect(event.date).to eq(Date.parse('2022-12-06'))
        expect(event.array_datetime).to be_a(Array)
        expect(event.array_datetime[0]).to be_a(DateTime)
        expect(event.array_datetime[0]).to eq('2022-12-06 15:22:49')
        expect(event.array_datetime[1]).to eq('2022-12-05 15:22:49')
      end
    end
  end
end
