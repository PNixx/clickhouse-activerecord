# frozen_string_literal: true

require 'spec_helper'
require 'clickhouse-activerecord/tasks'
require 'tempfile'
require 'timeout'

RSpec.describe ClickhouseActiverecord::StructureDumper, :migrations do
  let(:connection) { ActiveRecord::Base.connection }
  let(:db_config) { ActiveRecord::Base.connection_db_config }
  let(:database) { db_config.configuration_hash[:database] }

  # Names run backwards through the dependencies, so ordering by name - or by
  # kind and then by name - places every object before the ones it needs.
  let(:schema) do
    [
      'CREATE TABLE z_events (id UInt64, date Date, value UInt64) ENGINE = MergeTree ORDER BY (date, id)',
      'CREATE TABLE y_totals (date Date, total UInt64) ENGINE = SummingMergeTree ORDER BY date',
      'CREATE MATERIALIZED VIEW x_totals_mv TO y_totals AS SELECT date, sum(value) AS total FROM z_events GROUP BY date',
      'CREATE VIEW w_totals_view AS SELECT * FROM y_totals',
      'CREATE VIEW v_joined_view AS SELECT t.date AS date, e.id AS id FROM w_totals_view AS t INNER JOIN z_events AS e ON t.date = e.date',
      'CREATE MATERIALIZED VIEW u_ids_mv ENGINE = AggregatingMergeTree ORDER BY date AS SELECT date, uniqState(id) AS ids FROM v_joined_view GROUP BY date'
    ]
  end

  let(:dependencies) do
    {
      'x_totals_mv' => %w[y_totals z_events],
      'w_totals_view' => %w[y_totals],
      'v_joined_view' => %w[w_totals_view z_events],
      'u_ids_mv' => %w[v_joined_view]
    }
  end

  before { schema.each { |sql| connection.execute(sql) } }

  def schema_names
    dependencies.keys | %w[z_events y_totals]
  end

  def dumped_names(dumper = described_class.new(connection, database))
    dumper.statements.map { |sql| sql[/\ACREATE\s+(?:MATERIALIZED\s+VIEW|VIEW|DICTIONARY|TABLE|FUNCTION)\s+(\S+)/i, 1] }
  end

  it 'dumps every object after the objects it depends on' do
    names = dumped_names

    aggregate_failures do
      dependencies.each do |name, required|
        required.each do |dependency|
          expect(names.index(dependency)).to be < names.index(name),
                                             "expected #{dependency} before #{name}, got #{names.inspect}"
        end
      end
    end
  end

  it 'writes a structure file an empty database can be created from' do
    file = Tempfile.new(['structure', '.sql'])
    target = ActiveRecord::DatabaseConfigurations::HashConfig.new(
      db_config.env_name, db_config.name, db_config.configuration_hash.merge(database: "#{database}_structure_dump_spec")
    )

    ClickhouseActiverecord::Tasks.new(db_config).structure_dump(file.path)

    tasks = ClickhouseActiverecord::Tasks.new(target)

    begin
      tasks.create
      tasks.structure_load(file.path)
      expect(ActiveRecord::Base.connection.tables).to include(*schema_names)
    ensure
      tasks.drop
      ActiveRecord::Base.establish_connection(:default)
      file.close!
    end
  end

  context 'with user defined functions' do
    before do
      connection.drop_functions
      connection.execute('CREATE FUNCTION z_base_fun AS (x) -> x + 1')
      connection.execute('CREATE FUNCTION a_derived_fun AS (x) -> z_base_fun(x) * 2')
    end

    after { connection.drop_functions }

    it 'dumps functions before any table, and after the functions they call' do
      expect(dumped_names.first(2)).to eq(%w[z_base_fun a_derived_fun])
    end
  end

  it 'dumps every object even when the dependency graph has a cycle' do
    # ClickHouse rejects a genuine cycle, but a column carrying the name of a
    # table can still close one in the graph.
    cyclic = instance_double(ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
    allow(cyclic).to receive(:quote) { |value| "'#{value}'" }
    allow(cyclic).to receive(:execute) do |sql|
      { 'data' => sql.include?('system.functions') ? [] : [['a_view', [], [], [], []], ['b_view', [], [], [], []]] }
    end
    allow(cyclic).to receive(:show_create_table) do |name, **|
      other = name == 'a_view' ? 'b_view' : 'a_view'
      "CREATE VIEW #{name} AS SELECT * FROM #{other}"
    end

    names = Timeout.timeout(10) { dumped_names(described_class.new(cyclic, database)) }

    expect(names).to contain_exactly('a_view', 'b_view')
  end
end
