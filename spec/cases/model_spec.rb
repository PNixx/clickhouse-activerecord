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

    describe '#do_execute' do
      it 'returns formatted result' do
        result = model.connection.do_execute('SELECT 1 AS t')
        expect(result['data']).to eq([[1]])
        expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
      end

      context 'with JSONCompact format' do
        it 'returns formatted result' do
          result = model.connection.do_execute('SELECT 1 AS t', format: 'JSONCompact')
          expect(result['data']).to eq([[1]])
          expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
        end
      end

      context 'with JSONCompactEachRowWithNamesAndTypes format' do
        it 'returns formatted result' do
          result = model.connection.do_execute('SELECT 1 AS t', format: 'JSONCompactEachRowWithNamesAndTypes')
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

      it 'insert all' do
        if ActiveRecord::version >= Gem::Version.new('6')
          model.insert_all([
            {event_name: 'some event 1', date: date},
            {event_name: 'some event 2', date: date},
          ])
          expect(model.count).to eq(2)
        end
      end
    end

    describe '#update' do
      let(:record) { model.create!(event_name: 'some event', date: date) }

      it 'raises an error' do
        record.update!(event_name: 'new event name')
        expect(model.where(event_name: 'new event name').count).to eq(1)
      end
    end

    describe '#destroy' do
      let(:record) { model.create!(event_name: 'some event', date: date) }

      it 'raises an error' do
        record.destroy!
        expect(model.count).to eq(0)
      end
    end

    describe '#reverse_order!' do
      it 'blank' do
        expect(model.all.reverse_order!.map(&:event_name)).to eq([])
      end

      it 'select' do
        model.create!(event_name: 'some event 1', date: 1.day.ago)
        model.create!(event_name: 'some event 2', date: 2.day.ago)
        expect(model.all.reverse_order!.map(&:event_name)).to eq(['some event 1', 'some event 2'])
      end
    end

    describe 'convert type with aggregations' do
      let!(:record1) { model.create!(event_name: 'some event', event_value: 1, date: date) }
      let!(:record2) { model.create!(event_name: 'some event', event_value: 3, date: date) }

      it 'integer' do
        expect(model.select(Arel.sql('sum(event_value) AS event_value')).first.event_value.class).to eq(Integer)
        expect(model.select(Arel.sql('sum(event_value) AS value')).first.attributes['value'].class).to eq(Integer)
        expect(model.pluck(Arel.sql('sum(event_value)')).first[0].class).to eq(Integer)
      end
    end

    describe 'boolean column type' do
      let!(:record1) { model.create!(event_name: 'some event', event_value: 1, date: date) }

      it 'bool result' do
        expect(model.first.enabled.class).to eq(FalseClass)
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
    end

    describe '#settings' do
      it 'works' do
        sql = model.settings(optimize_read_in_order: 1, cast_keep_nullable: 1).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample SETTINGS optimize_read_in_order = 1, cast_keep_nullable = 1')
      end

      it 'quotes' do
        sql = model.settings(foo: :bar).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample SETTINGS foo = \'bar\'')
      end

      it 'allows passing the symbol :default to reset a setting' do
        sql = model.settings(max_insert_block_size: :default).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample SETTINGS max_insert_block_size = DEFAULT')
      end
    end

    describe '#using' do
      it 'works' do
        sql = model.joins(:joins).using(:event_name, :date).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample INNER JOIN joins USING event_name,date')
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

    describe 'DateTime64 create' do
      it 'create a new record' do
        time = DateTime.parse('2023-07-21 08:00:00.123')
        model.create!(datetime: time, datetime64: time)
        row = model.first
        expect(row.datetime).to_not eq(row.datetime64)
        expect(row.datetime.strftime('%Y-%m-%d %H:%M:%S')).to eq('2023-07-21 08:00:00')
        expect(row.datetime64.strftime('%Y-%m-%d %H:%M:%S.%3N')).to eq('2023-07-21 08:00:00.123')
      end
    end

    describe 'final request' do
      let!(:record1) { model.create!(date: date, event_name: '1') }
      let!(:record2) { model.create!(date: date, event_name: '1') }

      it 'select' do
        expect(model.count).to eq(2)
        expect(model.final.count).to eq(1)
        expect(model.final!.count).to eq(1)
        expect(model.final.where(date: '2023-07-21').to_sql).to eq('SELECT sample.* FROM sample FINAL WHERE sample.date = \'2023-07-21\'')
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
        expect(event.array_datetime.is_a?(Array)).to be_truthy
        expect(event.array_datetime[0].is_a?(DateTime)).to be_truthy
        expect(event.array_string[0].is_a?(String)).to be_truthy
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
