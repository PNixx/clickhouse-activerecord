require 'spec_helper'
require 'tmpdir'
require 'clickhouse-activerecord/tasks'

RSpec.describe 'Structure dump', :migrations do
  let(:connection) { ActiveRecord::Base.connection }
  let(:db_config) { ActiveRecord::Base.connection_db_config }
  let(:database) { db_config.database }
  let(:dump_path) { File.join(Dir.mktmpdir, 'structure.sql') }
  let(:dump) { File.read(dump_path) }

  before do
    connection.execute('CREATE TABLE events (id UInt64, name String) ENGINE = MergeTree ORDER BY id')
    connection.execute('CREATE TABLE event_names (name String) ENGINE = MergeTree ORDER BY name')
    connection.execute('CREATE MATERIALIZED VIEW events_mv TO event_names AS SELECT name FROM events')
    connection.execute('CREATE MATERIALIZED VIEW events_inner_mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM events')
    connection.execute('CREATE VIEW events_view AS SELECT id FROM events')
  end

  after do
    FileUtils.rm_rf(File.dirname(dump_path))
  end

  def dump_structure
    ClickhouseActiverecord::Tasks.new(db_config).structure_dump(dump_path)
  end

  it 'dumps every table and view without the database prefix' do
    dump_structure

    expect(dump).to include('CREATE TABLE events')
    expect(dump).to include('CREATE TABLE event_names')
    expect(dump).to include('CREATE MATERIALIZED VIEW events_mv')
    expect(dump).to include('CREATE VIEW events_view')
    expect(dump).not_to include("#{database}.")
  end

  it 'skips the inner tables of materialized views' do
    dump_structure

    expect(dump).not_to include('.inner_id.')
    expect(dump).not_to include('.inner.')
  end

  it 'orders tables before materialized views before views' do
    dump_structure

    expect(dump.index('CREATE TABLE events')).to be < dump.index('CREATE MATERIALIZED VIEW events_mv')
    expect(dump.index('CREATE MATERIALIZED VIEW events_mv')).to be < dump.index('CREATE VIEW events_view')
  end

  it 'writes the same definitions as SHOW CREATE TABLE' do
    dump_structure

    %w[events event_names events_mv events_view].each do |name|
      expect(dump).to include(connection.show_create_table(name, single_line: false))
    end
  end

  context 'with a server older than 23.10' do
    before do
      allow(connection).to receive(:server_version).and_return('23.9.1.1')
      allow(connection).to receive(:show_create_table).and_call_original
    end

    it 'reads each table definition with SHOW CREATE TABLE' do
      dump_structure

      %w[events event_names events_mv events_view].each do |name|
        expect(connection).to have_received(:show_create_table).with(name, single_line: false)
      end
    end

    it 'writes the same dump as a recent server' do
      dump_structure
      old_server_dump = dump

      allow(connection).to receive(:server_version).and_return('26.3.12.3')
      dump_structure

      expect(dump).to eq(old_server_dump)
    end
  end

  it 'reads the table definitions with a single query' do
    allow(connection).to receive(:show_create_table).and_call_original

    dump_structure

    expect(connection).not_to have_received(:show_create_table)
  end
end
