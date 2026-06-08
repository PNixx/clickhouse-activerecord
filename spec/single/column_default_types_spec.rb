# frozen_string_literal: true

RSpec.describe 'Column default types', :migrations do
  let(:connection) { ActiveRecord::Base.connection }

  before do
    connection.execute('DROP TABLE IF EXISTS column_default_types_test')
    connection.execute(<<~SQL.squish)
      CREATE TABLE column_default_types_test (
        id UInt64,
        name String DEFAULT '',
        computed String MATERIALIZED upper(name),
        scratch UInt64 EPHEMERAL 0,
        display String ALIAS lower(name)
      ) ENGINE = MergeTree ORDER BY id
    SQL
  end

  after do
    connection.execute('DROP TABLE IF EXISTS column_default_types_test')
  end

  let!(:model) do
    Class.new(ActiveRecord::Base) do
      self.table_name = 'column_default_types_test'
    end
  end

  describe 'Column#default_kind' do
    it 'is default? for DEFAULT columns' do
      col = model.columns_hash['name']
      expect(col.default_kind).to be_default
    end

    it 'is materialized? for MATERIALIZED columns' do
      col = model.columns_hash['computed']
      expect(col.default_kind).to be_materialized
    end

    it 'is alias? for ALIAS columns' do
      col = model.columns_hash['display']
      expect(col.default_kind).to be_alias
    end

    it 'is none? for plain columns' do
      col = model.columns_hash['id']
      expect(col.default_kind).to be_none
    end

    it 'does not expose EPHEMERAL columns in columns_hash' do
      expect(model.columns_hash).not_to have_key('scratch')
    end
  end

  describe 'INSERT behavior' do
    it 'creates a record successfully (excludes MATERIALIZED and ALIAS from INSERT)' do
      expect {
        model.create!(id: 1, name: 'hello')
      }.to change { model.count }.by(1)
    end

    it 'excludes MATERIALIZED columns even when dirtied' do
      record = model.new(id: 3, name: 'dirty')
      record[:computed] = 'MANUAL'
      expect { record.save! }.not_to raise_error
    end

    it 'excludes ALIAS columns even when dirtied' do
      record = model.new(id: 4, name: 'dirty')
      record[:display] = 'manual'
      expect { record.save! }.not_to raise_error
    end

    it 'creates a record with insert_all' do
      expect {
        model.insert_all([{ id: 2, name: 'world' }])
      }.to change { model.count }.by(1)
    end
  end

  describe 'SELECT behavior' do
    before { model.create!(id: 1, name: 'Hello') }

    it 'SELECT * does not include MATERIALIZED or ALIAS columns' do
      record = model.first
      expect(record.attributes).to include('id', 'name')
      expect(record.attributes).not_to include('computed', 'display')
    end

    it 'MATERIALIZED columns can be selected explicitly' do
      record = model.select(:id, :computed).first
      expect(record.computed).to eq('HELLO')
    end

    it 'ALIAS columns can be selected explicitly' do
      record = model.select(:id, :display).first
      expect(record.display).to eq('hello')
    end
  end

  describe 'WHERE behavior' do
    before { model.create!(id: 1, name: 'Hello') }

    it 'can filter on MATERIALIZED columns' do
      record = model.select(:id, :computed).where(computed: 'HELLO').first
      expect(record).to be_present
      expect(record.computed).to eq('HELLO')
    end

    it 'can filter on ALIAS columns' do
      record = model.select(:id, :display).where(display: 'hello').first
      expect(record).to be_present
      expect(record.display).to eq('hello')
    end
  end
end
