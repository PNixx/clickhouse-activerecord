# frozen_string_literal: true

require 'clickhouse-activerecord/tasks'

RSpec.describe ClickhouseActiverecord::Tasks, :migrations do
  let(:migrations_dir) { File.join(FIXTURES_PATH, 'migrations', 'structure_dump_ordering') }
  let(:tasks) { described_class.new(ActiveRecord::Base.connection_db_config) }
  let(:dump_path) { File.join(Dir.tmpdir, 'clickhouse_activerecord_structure.sql') }

  before do
    quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
  end

  after do
    ActiveRecord::Base.connection.drop_functions
  end

  def empty_database
    clear_db
    ActiveRecord::Base.connection.drop_functions
  end

  describe '#structure_dump' do
    subject(:structure_dump) { tasks.structure_dump(dump_path) }

    let(:dump) do
      structure_dump
      File.read(dump_path)
    end

    it 'dumps functions before tables' do
      expect(dump.index(/CREATE .*FUNCTION .*some_fun/)).to be < dump.index(/CREATE TABLE .*some_table_1/)
    end

    it 'dumps tables before materialized views' do
      expect(dump.index(/CREATE TABLE .*some_table_1/)).to be < dump.index(/CREATE MATERIALIZED VIEW .*some_mat_view/)
      expect(dump.index(/CREATE TABLE .*some_table_2/)).to be < dump.index(/CREATE MATERIALIZED VIEW .*some_mat_view/)
    end

    it 'dumps materialized views before views' do
      expect(dump.index(/CREATE MATERIALIZED VIEW .*some_mat_view/)).to be < dump.index(/CREATE VIEW .*some_view/)
    end
  end

  describe '#structure_load' do
    subject(:structure_load) { tasks.structure_load(dump_path) }

    before do
      tasks.structure_dump(dump_path)
      empty_database
    end

    it 'recreates functions, tables, materialized views, and views' do
      expect { structure_load }.not_to raise_error
      expect(ActiveRecord::Base.connection.functions).to include('some_fun')
      expect(ActiveRecord::Base.connection.tables).to include('some_table_1', 'some_table_2', 'some_mat_view', 'some_view')
    end
  end
end
