# frozen_string_literal: true

RSpec.describe 'Model', :migrations do
  let(:date) { Date.today }

  context 'sample' do
    let!(:model) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'sample'
      end
    end

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
    end

    describe '#with_response_format' do
      it 'returns formatted result' do
        result = model.connection.execute('SELECT 1 AS t')
        expect(result['data']).to eq([[1]])
        expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
      end

      context 'with JSONCompact format' do
        it 'returns formatted result' do
          result =
            model.connection.with_response_format('JSONCompact') do
              model.connection.execute('SELECT 1 AS t')
            end
          expect(result['data']).to eq([[1]])
          expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
        end
      end

      context 'with JSONCompactEachRowWithNamesAndTypes format' do
        it 'returns formatted result' do
          result =
            model.connection.with_response_format('JSONCompactEachRowWithNamesAndTypes') do
              model.connection.execute('SELECT 1 AS t')
            end
          expect(result['data']).to eq([[1]])
          expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
        end
      end
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

      before do
        model.connection.schema_cache.clear!
      end

      context 'when version is < 23.3' do
        before do
          allow(model.connection).to receive(:get_database_version).and_return(Gem::Version.new('23.2'))
        end

        it 'raises an error' do
          expect {
            record.update!(event_name: 'new event name')
          }.to raise_error(ActiveRecord::ActiveRecordError, 'ClickHouse update is not supported')
        end
      end

      context 'when version is >= 23.3' do
        before do
          allow(model.connection).to receive(:get_database_version).and_return(Gem::Version.new('23.3'))
        end

        it 'issues an ALTER TABLE...UPDATE statement' do
          captured = []
          allow(model.connection).to receive(:execute).and_wrap_original do |original_method, *args, **opts|
            captured << args.first
            original_method.call(*args, **opts) unless /^alter table/i.match?(args.first)
          end

          record.update!(event_name: 'new event name')

          expect(captured).to include(start_with("ALTER TABLE sample UPDATE event_name = 'new event name'"))
        end
      end
    end

    describe '#destroy' do
      let(:record) { model.create!(event_name: 'some event', date: date) }

      before do
        model.connection.schema_cache.clear!
      end

      context 'when version is < 23.3' do
        before do
          allow(model.connection).to receive(:get_database_version).and_return(Gem::Version.new('23.2'))
        end

        it 'raises an error' do
          expect { model.where(event_name: 'some event').delete_all }.to raise_error(ActiveRecord::ActiveRecordError, 'ClickHouse delete is not supported')
        end
      end

      context 'when version is >= 23.3' do
        before do
          allow(model.connection).to receive(:get_database_version).and_return(Gem::Version.new('23.3'))
        end

        it 'issues a DELETE statement' do
          captured = []
          allow(model.connection).to receive(:execute).and_wrap_original do |original_method, *args, **opts|
            captured << args.first
            original_method.call(*args, **opts) unless /^delete from/i.match?(args.first)
          end

          model.where(event_name: 'some event').delete_all

          expect(captured).to include("DELETE FROM sample WHERE event_name = 'some event'")
        end
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
        expect(model.first.enabled).to be_a(FalseClass)
      end

      it 'is mapped to :boolean' do
        type = model.columns_hash['enabled'].type
        expect(type).to eq(:boolean)
      end
    end

    describe 'string column type as byte array' do
      let(:bytes) { (0..255).to_a }
      let!(:record1) { model.create!(event_name: 'some event', byte_array: bytes.pack('C*')) }

      it 'keeps all bytes' do
        returned_byte_array = model.first.byte_array

        expect(returned_byte_array.unpack('C*')).to eq(bytes)
      end
    end

    describe 'UUID column type' do
      let(:random_uuid) { SecureRandom.uuid }
      let!(:record1) do
        model.create!(event_name: 'some event', event_value: 1, date: date, relation_uuid: random_uuid)
      end

      it 'is mapped to :uuid' do
        type = model.columns_hash['relation_uuid'].type
        expect(type).to eq(:uuid)
      end

      it 'accepts proper value' do
        expect(record1.relation_uuid).to eq(random_uuid)
      end

      it 'does not accept invalid values' do
        record1.relation_uuid = 'invalid-uuid'
        expect(record1.relation_uuid).to be_nil
      end

      it 'accepts non-canonical uuid' do
        record1.relation_uuid = 'ABCD-0123-4567-89EF-dead-beef-0101-1010'
        expect(record1.relation_uuid).to eq('abcd0123-4567-89ef-dead-beef01011010')
      end
    end

    describe 'final request' do
      it 'issues a FINAL query' do
        model.create!(date: date, event_name: '1')
        model.create!(date: date, event_name: '1')

        expect(model.count).to eq(2)
        expect(model.final.count).to eq(1)
        expect(model.final!.count).to eq(1)
        expect(model.final.where(date: '2023-07-21').to_sql).to eq('SELECT sample.* FROM sample FINAL WHERE sample.date = \'2023-07-21\'')
      end
    end

    describe 'arel predicates' do
      describe '#matches' do
        it 'uses ilike for case insensitive matches' do
          sql = model.where(model.arel_table[:event_name].matches('some event')).to_sql
          expect(sql).to eq("SELECT sample.* FROM sample WHERE sample.event_name ILIKE 'some event'")
        end

        it 'uses like for case sensitive matches' do
          sql = model.where(model.arel_table[:event_name].matches('some event', nil, true)).to_sql
          expect(sql).to eq("SELECT sample.* FROM sample WHERE sample.event_name LIKE 'some event'")
        end
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
            array_int: [1, 2],
            date: date
          )
        }.to change { model.count }
        event = model.first
        expect(event.array_datetime).to be_a(Array)
        expect(event.array_datetime[0]).to be_a(DateTime)
        expect(event.array_string[0]).to be_a(String)
        expect(event.array_string).to eq(%w[asdf jkl])
        expect(event.array_int.is_a?(Array)).to be_truthy
        expect(event.array_int).to eq([1, 2])
      end

      it 'create with insert all' do
        expect {
          model.insert_all([{
                              array_datetime: [1.day.ago, Time.now, '2022-12-06 15:22:49'],
                              array_string: %w[asdf jkl],
                              array_int: [1, 2],
                              date: date
                            }])
        }.to change { model.count }
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
