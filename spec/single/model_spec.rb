# frozen_string_literal: true

RSpec.describe 'Model', :migrations do

  class ModelJoin < ActiveRecord::Base
    self.table_name = 'joins'
    belongs_to :model, class_name: 'Model'
  end
  class Model < ActiveRecord::Base
    self.table_name = 'sample'
    has_many :joins, class_name: 'ModelJoin', primary_key: 'event_name'
  end
  class ModelPk < ActiveRecord::Base
    self.table_name = 'sample'
    self.primary_key = 'event_name'
  end
  IS_NEW_CLICKHOUSE_SERVER = Model.connection.server_version.to_f >= 23.4

  let(:date) { Date.today }

  context 'sample' do

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
    end

    if IS_NEW_CLICKHOUSE_SERVER
      it "detect primary key" do
        expect(Model.primary_key).to eq('event_name')
      end
    end

    it 'DB::Exception in row value' do
      Model.create!(event_name: 'DB::Exception')
      expect(Model.first.event_name).to eq('DB::Exception')
    end

    describe '#execute' do
      it 'returns formatted result' do
        result = Model.connection.execute('SELECT 1 AS t')
        expect(result['data']).to eq([[1]])
        expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
      end

      it 'also works when a different format is passed as a keyword' do
        result = Model.connection.execute('SELECT 1 AS t', format: 'JSONCompact')
        expect(result['data']).to eq([[1]])
        expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
      end
    end

    describe '#with_response_format' do
      it 'returns formatted result' do
        result = Model.connection.execute('SELECT 1 AS t')
        expect(result['data']).to eq([[1]])
        expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
      end

      context 'with JSONCompact format' do
        it 'returns formatted result' do
          result =
            Model.connection.with_response_format('JSONCompact') do
              Model.connection.execute('SELECT 1 AS t')
            end
          expect(result['data']).to eq([[1]])
          expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
        end
      end

      context 'with JSONCompactEachRowWithNamesAndTypes format' do
        it 'returns formatted result' do
          result =
            Model.connection.with_response_format('JSONCompactEachRowWithNamesAndTypes') do
              Model.connection.execute('SELECT 1 AS t')
            end
          expect(result['data']).to eq([[1]])
          expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
        end
      end

      context 'with nil format' do
        it 'omits the FORMAT clause' do
          result =
            Model.connection.with_response_format(nil) do
              Model.connection.execute('SELECT 1 AS t')
            end
          expect(result.chomp).to eq('1')
        end
      end
    end

    describe '#create' do
      it 'creates a new record' do
        expect {
          Model.create!(
            event_name: 'some event',
            date: date
          )
        }.to change { Model.count }
      end

      it 'insert all' do
        if ActiveRecord::version >= Gem::Version.new('6')
          Model.insert_all([
            {event_name: 'some event 1', date: date},
            {event_name: 'some event 2', date: date},
          ])
          expect(Model.count).to eq(2)
        end
      end
    end

    describe '#update' do
      let!(:record) { Model.create!(event_name: 'some event', event_value: 1, date: date) }

      it 'update' do
        expect {
          Model.where(event_name: 'some event').update_all(event_value: 2)
        }.to_not raise_error
      end

      it 'update model with primary key' do
        expect {
          if IS_NEW_CLICKHOUSE_SERVER
            Model.first.update!(event_value: 2)
          else
            ModelPk.first.update!(event_value: 2)
          end
        }.to_not raise_error
      end
    end

    describe '#delete' do
      let!(:record) { Model.create!(event_name: 'some event', date: date) }

      it 'scope' do
        expect {
          Model.where(event_name: 'some event').delete_all
        }.to_not raise_error
      end

      it 'destroy model with primary key' do
        expect {
          if IS_NEW_CLICKHOUSE_SERVER
            Model.first.destroy!
          else
            ModelPk.first.destroy!
          end
        }.to_not raise_error
      end
    end

    describe '#find_by' do
      let!(:record) { Model.create!(event_name: 'some event', date: Date.current, datetime: Time.now) }

      it 'finds the record' do
        expect(Model.find_by(event_name: 'some event').attributes).to eq(record.attributes)
      end

      it 'find with record `insert into table`' do
        Model.create!(event_name: 'INSERT INTO table VALUES(1,1)', date: Date.current, datetime: Time.now)
        expect(Model.where('event_name ILIKE ?', 'insert into%').count).to eq(1)
      end
    end

    describe '#reverse_order!' do
      it 'blank' do
        expect(Model.all.reverse_order!.map(&:event_name)).to eq([])
      end

      it 'select' do
        Model.create!(event_name: 'some event 1', date: 1.day.ago)
        Model.create!(event_name: 'some event 2', date: 2.day.ago)
        if IS_NEW_CLICKHOUSE_SERVER
          expect(Model.all.reverse_order!.to_sql).to eq('SELECT sample.* FROM sample ORDER BY sample.event_name DESC')
          expect(Model.all.reverse_order!.map(&:event_name)).to eq(['some event 2', 'some event 1'])
        else
          expect(Model.all.reverse_order!.to_sql).to eq('SELECT sample.* FROM sample ORDER BY sample.date DESC')
          expect(Model.all.reverse_order!.map(&:event_name)).to eq(['some event 1', 'some event 2'])
        end
      end
    end

    describe 'convert type with aggregations' do
      let!(:record1) { Model.create!(event_name: 'some event', event_value: 1, date: date) }
      let!(:record2) { Model.create!(event_name: 'some event', event_value: 3, date: date) }

      it 'integer' do
        expect(Model.select(Arel.sql('sum(event_value) AS event_value'))[0].event_value.class).to eq(Integer)
        expect(Model.select(Arel.sql('sum(event_value) AS value'))[0].attributes['value'].class).to eq(Integer)
        expect(Model.pluck(Arel.sql('sum(event_value)')).first[0].class).to eq(Integer)
      end
    end

    describe 'boolean column type' do
      let!(:record1) { Model.create!(event_name: 'some event', event_value: 1, date: date) }

      it 'bool result' do
        expect(Model.first.enabled.class).to eq(FalseClass)
      end

      it 'is mapped to :boolean' do
        type = Model.columns_hash['enabled'].type
        expect(type).to eq(:boolean)
      end
    end

    describe 'string column type as byte array' do
      let(:bytes) { (0..255).to_a }
      let!(:record1) { Model.create!(event_name: 'some event', byte_array: bytes.pack('C*')) }

      it 'keeps all bytes' do
        returned_byte_array = Model.first.byte_array

        expect(returned_byte_array.unpack('C*')).to eq(bytes)
      end
    end

    describe 'UUID column type' do
      let(:random_uuid) { SecureRandom.uuid }
      let!(:record1) do
        Model.create!(event_name: 'some event', event_value: 1, date: date, relation_uuid: random_uuid)
      end

      it 'is mapped to :uuid' do
        type = Model.columns_hash['relation_uuid'].type
        expect(type).to eq(:uuid)
      end

      it 'accepts proper value' do
        expect(record1.relation_uuid).to eq(random_uuid)
      end

      it 'accepts non-canonical uuid' do
        record1.relation_uuid = 'ABCD-0123-4567-89EF-dead-beef-0101-1010'
        expect(record1.relation_uuid).to eq('abcd0123-4567-89ef-dead-beef01011010')
      end

      it 'does not accept invalid values' do
        record1.relation_uuid = 'invalid-uuid'
        expect(record1.relation_uuid).to be_nil
      end
    end

    describe 'decimal column type' do
      let!(:record1) do
        Model.create!(event_name: 'some event', decimal_value: BigDecimal('95891.74'))
      end

      # If converted to float, the value would be 9589174.000000001. This happened previously
      # due to JSON parsing of numeric values to floats.
      it 'keeps precision' do
        decimal_value = Model.first.decimal_value
        expect(decimal_value).to eq(BigDecimal('95891.74'))
      end
    end

    describe '#settings' do
      it 'does not change settings_values when empty' do
        query = Model.all
        query = query.settings
        expect(query.settings_values).to be_empty
      end

      it 'updates settings_values' do
        query = Model.all
        query = query.settings(foo: 'bar', abc: 123)
        expect(query.settings_values).to eq({ foo: 'bar', abc: 123 })
      end

      it 'overwrites settings_values previously set' do
        query = Model.all
        query = query.settings(foo: 'bar', abc: 123)
        query = query.settings(foo: 'baz')
        expect(query.settings_values).to eq({ foo: 'baz', abc: 123 })
      end

      it 'works as a chainable method' do
        query = Model.all
        query = query.settings(foo: 'bar', abc: 123).settings(foo: 'baz')
        expect(query.settings_values).to eq({ foo: 'baz', abc: 123 })
      end

      it 'works' do
        sql = Model.settings(optimize_read_in_order: 1, cast_keep_nullable: 1).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample SETTINGS optimize_read_in_order = 1, cast_keep_nullable = 1')
      end

      it 'quotes' do
        sql = Model.settings(foo: :bar).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample SETTINGS foo = \'bar\'')
      end

      it 'allows passing the symbol :default to reset a setting' do
        sql = Model.settings(max_insert_block_size: :default).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample SETTINGS max_insert_block_size = DEFAULT')
      end
    end

    describe 'block-style settings' do
      let!(:record) { Model.create!(event_name: 'some event', date: Date.current, datetime: Time.now) }

      let(:last_query_finder) do
        <<~SQL.squish
          SELECT query, Settings, event_time_microseconds
          FROM system.query_log
          WHERE query ILIKE 'SELECT sample.* FROM sample FORMAT %'
          ORDER BY event_date DESC, event_time DESC, event_time_microseconds DESC
          LIMIT 1
        SQL
      end

      it 'sends the settings to the server' do
        expect_any_instance_of(Net::HTTP).to receive(:post).and_wrap_original do |original_method, *args, **kwargs|
          resource, sql, * = args
          if sql.include?('SELECT sample.*')
            query = resource.split('?').second
            params = query.split('&').to_h { |pair| pair.split('=').map { |s| CGI.unescape(s) } }
            expect(params['cast_keep_nullable']).to eq('1')
            expect(params['log_comment']).to eq('Log Comment!')
          end
          original_method.call(*args, **kwargs)
        end

        Model.connection.with_settings(cast_keep_nullable: 1, log_comment: 'Log Comment!') do
          Model.all.load
        end
      end

      it 'resets settings to default outside the block' do
        Model.connection.with_settings(cast_keep_nullable: 1, log_comment: 'Log Comment!') do
          Model.all.load
        end

        expect_any_instance_of(Net::HTTP).to receive(:post).and_wrap_original do |original_method, *args, **kwargs|
          resource, sql, * = args
          if sql.include?('SELECT sample.*')
            query = resource.split('?').second
            params = query.split('&').to_h { |pair| pair.split('=').map { |s| CGI.unescape(s) } }
            expect(params).not_to include('cast_keep_nullable', 'log_comment')
          end
          original_method.call(*args, **kwargs)
        end

        Model.all.load
      end
    end

    describe '#using' do
      it 'works' do
        sql = Model.joins(:joins).using(:event_name, :date).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample INNER JOIN joins USING event_name,date')
      end

      it 'works with filters' do
        sql = Model.joins(:joins).using(:event_name, :date).where(joins: { event_value: 1 }).to_sql
        expect(sql).to eq("SELECT sample.* FROM sample INNER JOIN joins USING event_name,date WHERE joins.event_value = 1")
      end
    end

    describe '#window' do
      it 'works' do
        sql = Model.window('x', order: 'date', partition: 'name', rows: 'UNBOUNDED PRECEDING').select('sum(event_value) OVER x').to_sql
        expect(sql).to eq('SELECT sum(event_value) OVER x FROM sample WINDOW x AS (PARTITION BY name ORDER BY date ROWS UNBOUNDED PRECEDING)')
      end

      it 'empty' do
        sql = Model.window('x').select('sum(event_value) OVER x').to_sql
        expect(sql).to eq('SELECT sum(event_value) OVER x FROM sample WINDOW x AS ()')
      end
    end

    describe '#unscope' do
      it 'removes settings' do
        sql = Model.settings(foo: :bar).unscope(:settings).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample')
      end

      it 'removes FINAL' do
        sql = Model.final.unscope(:final).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample')
      end
    end

    describe 'arel predicates' do
      describe '#matches' do
        it 'uses ilike for case insensitive matches' do
          sql = Model.where(Model.arel_table[:event_name].matches('some event')).to_sql
          expect(sql).to eq("SELECT sample.* FROM sample WHERE sample.event_name ILIKE 'some event'")
        end

        it 'uses like for case sensitive matches' do
          sql = Model.where(Model.arel_table[:event_name].matches('some event', nil, true)).to_sql
          expect(sql).to eq("SELECT sample.* FROM sample WHERE sample.event_name LIKE 'some event'")
        end
      end
    end

    describe 'DateTime64 create' do
      it 'create a new record' do
        time = DateTime.parse('2023-07-21 08:00:00.123')
        Model.create!(datetime: time, datetime64: time)
        row = Model.first
        expect(row.datetime).to_not eq(row.datetime64)
        expect(row.datetime.strftime('%Y-%m-%d %H:%M:%S')).to eq('2023-07-21 08:00:00')
        expect(row.datetime64.strftime('%Y-%m-%d %H:%M:%S.%3N')).to eq('2023-07-21 08:00:00.123')
      end
    end

    describe 'final request' do
      let!(:record1) { Model.create!(date: date, event_name: '1') }
      let!(:record2) { Model.create!(date: date, event_name: '1') }

      it 'select' do
        expect(Model.count).to eq(2)
        expect(Model.final.count).to eq(1)
        expect(Model.final!.count).to eq(1)
        expect(Model.final.where(date: '2023-07-21').to_sql).to eq('SELECT sample.* FROM sample FINAL WHERE sample.date = \'2023-07-21\'')
      end

      it 'works with JOINs' do
        sql = Model.final.joins(:joins).where(date: '2023-07-21').to_sql
        expect(sql).to eq('SELECT sample.* FROM sample FINAL INNER JOIN joins ON joins.model_id = sample.event_name WHERE sample.date = \'2023-07-21\'')
      end
    end

    describe '#limit_by' do
      it 'works' do
        sql = Model.limit_by(1, :event_name).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample LIMIT 1 BY event_name')
      end

      it 'works with limit' do
        sql = Model.limit(1).limit_by(1, :event_name).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample LIMIT 1 BY event_name LIMIT 1')
      end
    end

    describe '#group_by_grouping_sets' do
      it 'raises an error with no arguments' do
        expect { Model.group_by_grouping_sets }.to raise_error(ArgumentError, 'The method .group_by_grouping_sets() must contain arguments.')
      end

      it 'works with the empty grouping set' do
        sql = Model.group_by_grouping_sets([]).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample GROUP BY GROUPING SETS ( (  ) )')
      end

      it 'accepts strings' do
        sql = Model.group_by_grouping_sets(%w[foo bar], %w[baz]).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample GROUP BY GROUPING SETS ( ( foo, bar ), ( baz ) )')
      end

      it 'accepts symbols' do
        sql = Model.group_by_grouping_sets(%i[foo bar], %i[baz]).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample GROUP BY GROUPING SETS ( ( foo, bar ), ( baz ) )')
      end

      it 'accepts Arel nodes' do
        sql = Model.group_by_grouping_sets([Model.arel_table[:foo], Model.arel_table[:bar]], [Model.arel_table[:baz]]).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample GROUP BY GROUPING SETS ( ( sample.foo, sample.bar ), ( sample.baz ) )')
      end

      it 'accepts mixed arguments' do
        sql = Model.group_by_grouping_sets(['foo', :bar], [Model.arel_table[:baz]]).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample GROUP BY GROUPING SETS ( ( foo, bar ), ( sample.baz ) )')
      end
    end

    describe '#cte & cse:' do
      it 'cte string' do
        sql = Model.with('t' => ModelJoin.where(event_name: 'test')).where(event_name: Model.from('t').select('event_name')).to_sql
        expect(sql).to eq('WITH t AS (SELECT joins.* FROM joins WHERE joins.event_name = \'test\') SELECT sample.* FROM sample WHERE sample.event_name IN (SELECT event_name FROM t)')
      end

      it 'cte symbol' do
        sql = Model.with(t: ModelJoin.where(event_name: 'test')).where(event_name: Model.from('t').select('event_name')).to_sql
        expect(sql).to eq('WITH t AS (SELECT joins.* FROM joins WHERE joins.event_name = \'test\') SELECT sample.* FROM sample WHERE sample.event_name IN (SELECT event_name FROM t)')
      end

      it 'cse string variable' do
        sql = Model.with('2026-01-01 15:23:00' => :t).where(Arel.sql('date = toDate(t)')).to_sql
        expect(sql).to eq('WITH \'2026-01-01 15:23:00\' AS t SELECT sample.* FROM sample WHERE (date = toDate(t))')
      end

      it 'cse symbol function' do
        sql = Model.with('(id, extension) -> concat(lower(id), extension)': :t).where(Arel.sql('date = toDate(t)')).to_sql
        expect(sql).to eq('WITH (id, extension) -> concat(lower(id), extension) AS t SELECT sample.* FROM sample WHERE (date = toDate(t))')
      end

      it 'cse query relation' do
        sql = Model.with(ModelJoin.select(Arel.sql('min(date)')) => :min_date).where(Arel.sql('date = min_date')).to_sql
        expect(sql).to eq('WITH (SELECT min(date) FROM joins) AS min_date SELECT sample.* FROM sample WHERE (date = min_date)')
      end

      it 'cse error' do
        expect { Model.with('2026-01-01 15:23:00' => 't').where(Arel.sql('date = toDate(t)')).to_sql }.to raise_error(ArgumentError)
      end
    end
  end

  context 'sample with id column' do
    class ModelWithoutPrimaryKey < ActiveRecord::Base
      self.table_name = 'sample_without_key'
    end

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data_without_primary_key')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
    end

    it 'detect primary key' do
      expect(ModelWithoutPrimaryKey.primary_key).to eq(nil)
    end

    describe '#delete' do
      let!(:record) { ModelWithoutPrimaryKey.create!(event_name: 'some event', date: date) }

      it 'model destroy' do
        expect {
          record.destroy!
        }.to raise_error(ActiveRecord::ActiveRecordError, 'Deleting a row is not possible without a primary key')
      end

      it 'scope' do
        expect {
          ModelWithoutPrimaryKey.where(event_name: 'some event').delete_all
        }.to_not raise_error
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
      quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
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

  context 'map' do
    let!(:model) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'verbs'
      end
    end

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_map_datetime')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
    end

    describe '#create' do
      it 'creates a new record' do
        expect {
          model.create!(
            map_datetime: {a: 1.day.ago, b: Time.now, c: '2022-12-06 15:22:49'},
            map_string: {a: 'asdf', b: 'jkl' },
            map_int: {a: 1, b: 2},
            map_array_datetime: {a: [1.day.ago], b: [Time.now, '2022-12-06 15:22:49']},
            map_array_string: {a: ['str'], b: ['str1', 'str2']},
            map_array_int: {a: [1], b: [1, 2, 3]},
            date: date
          )
        }.to change { model.count }.by(1)

        record = model.first
        expect(record.map_datetime).to be_a Hash
        expect(record.map_string).to be_a Hash
        expect(record.map_int).to be_a Hash
        expect(record.map_array_datetime).to be_a Hash
        expect(record.map_array_string).to be_a Hash
        expect(record.map_array_int).to be_a Hash

        expect(record.map_datetime['a']).to be_a DateTime
        expect(record.map_string['a']).to be_a String
        expect(record.map_string).to eq({'a' => 'asdf', 'b' => 'jkl'})
        expect(record.map_int).to eq({'a' => 1, 'b' => 2})

        expect(record.map_array_datetime['b']).to be_a Array
        expect(record.map_array_string['b']).to be_a Array
        expect(record.map_array_int['b']).to be_a Array
      end

      it 'create with insert all' do
        expect {
          model.insert_all([{
            map_datetime: {a: 1.day.ago, b: Time.now, c: '2022-12-06 15:22:49'},
            map_string: {a: 'asdf', b: 'jkl' },
            map_int: {a: 1, b: 2},
            map_array_datetime: {a: [1.day.ago], b: [Time.now, '2022-12-06 15:22:49']},
            map_array_string: {a: ['str'], b: ['str1', 'str2']},
            map_array_int: {a: [1], b: [1, 2, 3]},
            date: date
          }])
        }.to change { model.count }.by(1)
      end

      it 'get record' do
        model.connection.insert("INSERT INTO #{model.table_name} (id, map_datetime, map_array_datetime, date) VALUES (1, {'a': '2022-12-05 15:22:49', 'b': '2024-01-01 12:00:08'}, {'c': ['2022-12-05 15:22:49','2024-01-01 12:00:08']}, '2022-12-06')")
        expect(model.count).to eq(1)
        record = model.first
        expect(record.date.is_a?(Date)).to be_truthy
        expect(record.date).to eq(Date.parse('2022-12-06'))
        expect(record.map_datetime).to be_a Hash
        expect(record.map_datetime['a'].is_a?(DateTime)).to be_truthy
        expect(record.map_datetime['a']).to eq(DateTime.parse('2022-12-05 15:22:49'))
        expect(record.map_datetime['b']).to eq(DateTime.parse('2024-01-01 12:00:08'))
        expect(record.map_array_datetime).to be_a Hash
        expect(record.map_array_datetime['c']).to be_a Array
        expect(record.map_array_datetime['c'][0]).to eq(DateTime.parse('2022-12-05 15:22:49'))
        expect(record.map_array_datetime['c'][1]).to eq(DateTime.parse('2024-01-01 12:00:08'))
      end
    end
  end

  if Model.connection.server_version.to_f > 24.6
    context 'json' do
      let!(:json_model) do
        Class.new(ActiveRecord::Base) do
          self.table_name = 'json_test_table'
        end
      end

      before do
        # Create table with JSON column
        json_model.connection.execute('DROP TABLE IF EXISTS json_test_table')
        json_model.connection.execute(<<~SQL, nil, settings: { allow_experimental_json_type: 1 })
          CREATE TABLE json_test_table (
            id UInt64,
            properties JSON,
            metadata JSON
          ) ENGINE = MergeTree ORDER BY id
        SQL
      end

      after do
        json_model.connection.execute('DROP TABLE IF EXISTS json_test_table')
      end

      describe 'JSON column type recognition' do
        it 'recognizes JSON columns with correct type' do
          columns = json_model.columns_hash
          expect(columns['properties'].type).to eq(:json)
          expect(columns['properties'].sql_type).to eq('JSON')
          expect(columns['metadata'].type).to eq(:json)
        end

        it 'validates JSON type in connection' do
          connection = json_model.connection
          expect(connection.valid_type?(:json)).to be_truthy
          expect(connection.native_database_types[:json]).to eq({ name: 'JSON' })
        end
      end

      describe 'JSON data operations' do
        it 'creates record with JSON data' do
          test_data = { 'key' => 'value', 'nested' => { 'count' => '42' } }
          metadata = { 'version' => '1.0', 'tags' => ['test', 'json'] }

          expect {
            json_model.create!(
              id: 1,
              properties: test_data,
              metadata: metadata
            )
          }.to change { json_model.count }.by(1)

          record = json_model.first
          expect(record.properties).to eq(test_data)
          expect(record.metadata).to eq(metadata)
        end

        it 'handles empty JSON values' do
          expect {
            json_model.create!(
              id: 2,
              properties: {},
              metadata: { 'status' => 'empty' }
            )
          }.to change { json_model.count }.by(1)

          record = json_model.first
          expect(record.properties).to eq({})
          expect(record.metadata).to eq({ 'status' => 'empty' })
        end

        it 'handles complex JSON structures' do
          # Note: In ClickHouse JSON type, numbers are stored as strings
          complex_json = {
            'user' => {
              'name' => 'John Doe',
              'preferences' => {
                'theme' => 'dark',
                'notifications' => true,
                'languages' => ['en', 'es']
              }
            },
            'timestamps' => {
              'created_at' => '2023-01-01T00:00:00Z',
              'updated_at' => '2023-12-31T23:59:59Z'
            },
            'metrics' => ['1', '2', '3', '4', '5'],  # Numbers become strings in ClickHouse JSON
            'active' => true,
            'score' => 0.955e2  # ClickHouse JSON representation
          }

          json_model.create!(
            id: 3,
            properties: complex_json,
            metadata: { 'type' => 'complex' }
          )

          record = json_model.first
          expect(record.properties['user']['name']).to eq('John Doe')
          expect(record.properties['metrics']).to eq(['1', '2', '3', '4', '5'])
          expect(record.properties['active']).to be_truthy
          expect(record.properties['score']).to be_a(Numeric)
        end

        it 'works with insert_all' do
          records = [
            { id: 4, properties: { 'batch' => '1' }, metadata: { 'source' => 'batch' } },
            { id: 5, properties: { 'batch' => '2' }, metadata: { 'source' => 'batch' } }
          ]

          expect {
            json_model.insert_all(records)
          }.to change { json_model.count }.by(2)

          first_record = json_model.find_by(id: 4)
          second_record = json_model.find_by(id: 5)

          expect(first_record.properties).to eq({ 'batch' => '1' })
          expect(second_record.properties).to eq({ 'batch' => '2' })
          expect(first_record.metadata).to eq({ 'source' => 'batch' })
        end
      end

      describe 'migration and schema dumping' do
        it 'allows creating tables with JSON columns via migration' do
          # Create a temporary migration-style table
          json_model.connection.execute('DROP TABLE IF EXISTS migration_json_test')

          expect {
            json_model.connection.create_table :migration_json_test, id: false,
                                              options: 'MergeTree ORDER BY id',
                                              request_settings: { allow_experimental_json_type: 1 } do |t|
              t.column :id, :integer, null: false
              t.json :config, null: false
              t.json :optional_data, null: false  # JSON columns cannot be nullable in ClickHouse
            end
          }.not_to raise_error

          # Verify the table was created with correct column types
          columns = json_model.connection.columns('migration_json_test')
          config_column = columns.find { |c| c.name == 'config' }
          optional_column = columns.find { |c| c.name == 'optional_data' }

          expect(config_column.type).to eq(:json)
          expect(config_column.sql_type).to eq('JSON')
          expect(optional_column.type).to eq(:json)

          json_model.connection.execute('DROP TABLE IF EXISTS migration_json_test')
        end
      end
    end
  end
end
