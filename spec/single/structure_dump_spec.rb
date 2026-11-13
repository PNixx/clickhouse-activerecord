# frozen_string_literal: true

require 'spec_helper'
require 'clickhouse-activerecord/tasks'
require 'tempfile'
require 'timeout'

RSpec.describe 'Structure dump', :migrations do
  let(:connection) { ActiveRecord::Base.connection }
  let(:database) { ActiveRecord::Base.connection_db_config.database }
  let(:tasks) { ClickhouseActiverecord::Tasks.new(ActiveRecord::Base.connection_db_config) }

  after { connection.execute('DROP FUNCTION IF EXISTS structure_spec_double', nil, format: nil) }

  def dump
    Tempfile.create(['structure', '.sql']) do |file|
      tasks.structure_dump(file.path)
      File.read(file.path)
    end
  end

  # Loads the structure into an empty database. The suite shares one database, so this empties it
  # table by table and keeps the database itself.
  def load_structure(structure)
    Tempfile.create(['structure', '.sql']) do |file|
      file.write(structure)
      file.flush
      connection.execute('DROP VIEW IF EXISTS structure_spec_view')
      clear_db
      tasks.structure_load(file.path)
    end
  end

  def show_create_tables
    connection.tables.sort.to_h { |name| [name, connection.show_create_table(name)] }
  end

  it 'loads back computed columns, codecs, time zones and views, and dumps them the same' do
    connection.execute(<<~SQL)
      CREATE TABLE structure_spec (
        id UInt64,
        version DateTime64(6) DEFAULT now64(6),
        happened_at DateTime64(6, 'UTC'),
        metadata String,
        campaign_type LowCardinality(String) MATERIALIZED JSONExtractString(metadata, 'campaign_type'),
        compressed UInt32 MATERIALIZED length(metadata) CODEC(ZSTD(1)),
        doubled UInt64 ALIAS id * 2,
        amount Decimal(18, 2) DEFAULT 0
      ) ENGINE = ReplacingMergeTree(version) ORDER BY id
    SQL
    connection.execute('CREATE VIEW structure_spec_view AS SELECT id FROM structure_spec')
    tables = show_create_tables
    structure = dump

    load_structure(structure)

    expect(show_create_tables).to eq(tables)
    expect(dump).to eq(structure)
  end

  it 'keeps the strings that mention the database: defaults, Enum labels and comments' do
    connection.execute(<<~SQL)
      CREATE TABLE structure_spec (
        id UInt64,
        source String DEFAULT '#{database}.example' COMMENT '#{database}.comment',
        note String DEFAULT 'copied from #{database}.example',
        kind Enum8('#{database}.web' = 1)
      ) ENGINE = MergeTree ORDER BY id
    SQL

    load_structure(dump)

    columns = connection.execute(<<~SQL)['data']
      SELECT name, type, default_expression, comment FROM system.columns
      WHERE database = '#{database}' AND table = 'structure_spec' AND name != 'id' ORDER BY name
    SQL
    expect(columns).to eq([
      ['kind', "Enum8('#{database}.web' = 1)", '', ''],
      ['note', 'String', "'copied from #{database}.example'", ''],
      ['source', 'String', "'#{database}.example'", "#{database}.comment"]
    ])
  end

  it 'keeps a backquoted name with a dot, and a qualifier that only ends in the database name' do
    other = "other_#{database}"
    connection.execute("CREATE DATABASE #{other}")
    connection.execute("CREATE TABLE #{other}.structure_spec (id UInt64) ENGINE = MergeTree ORDER BY id")
    connection.execute("CREATE VIEW structure_spec_view AS SELECT id AS `x #{database}.id` FROM #{other}.structure_spec")

    expect(connection.show_create_table('structure_spec_view')).to include("`x #{database}.id`", "FROM #{other}.structure_spec")
  ensure
    connection.execute('DROP VIEW IF EXISTS structure_spec_view')
    connection.execute("DROP DATABASE IF EXISTS #{other}")
  end

  it 'keeps the SQL unchanged when the configuration names no database' do
    # Rails 7.1 connects through `clickhouse_connection`, which needs the database key.
    ActiveRecord::Base.establish_connection(ActiveRecord::Base.connection_db_config.configuration_hash.merge(database: nil))
    ActiveRecord::Base.connection.execute(<<~SQL)
      CREATE TABLE default.structure_spec (id UInt64, extension String DEFAULT '.csv') ENGINE = MergeTree ORDER BY id
    SQL

    server_sql = ActiveRecord::Base.connection.execute('SHOW CREATE TABLE structure_spec')['data'].first.first
    expect(ActiveRecord::Base.connection.show_create_table('structure_spec', single_line: false)).to eq(server_sql)
  ensure
    ActiveRecord::Base.connection.execute('DROP TABLE IF EXISTS default.structure_spec')
    ActiveRecord::Base.establish_connection(:default)
  end

  it 'loads into a database with another name, with names qualified for that database' do
    other = "#{database}_structure_spec"
    connection.execute('CREATE TABLE structure_spec (id UInt64, name String) ENGINE = MergeTree ORDER BY id')
    connection.execute(<<~SQL)
      CREATE DICTIONARY `structure-spec-dict` (id UInt64, name String) PRIMARY KEY id
      SOURCE(CLICKHOUSE(TABLE 'structure_spec')) LIFETIME(0) LAYOUT(FLAT())
    SQL
    connection.execute(<<~SQL)
      CREATE VIEW structure_spec_view AS SELECT
        dictGet('#{database}.structure-spec-dict', 'name', id) AS name,
        dictHas('#{database}.structure-spec-dict', id) AS known,
        dictGetOrDefault('#{database}.structure-spec-dict', 'name', id, '#{database}.unknown') AS name_or_default
      FROM structure_spec
    SQL
    structure = dump

    connection.execute("CREATE DATABASE #{other}")
    ActiveRecord::Base.establish_connection(ActiveRecord::Base.connection_db_config.configuration_hash.merge(database: other))
    Tempfile.create(['structure', '.sql']) do |file|
      file.write(structure)
      file.flush
      tasks.structure_load(file.path)
    end

    view = ActiveRecord::Base.connection.select_value(
      "SELECT create_table_query FROM system.tables WHERE database = '#{other}' AND name = 'structure_spec_view'"
    )
    expect(view).to include(
      "dictGet('#{other}.structure-spec-dict'",
      "dictHas('#{other}.structure-spec-dict'",
      "dictGetOrDefault('#{other}.structure-spec-dict', 'name', id, '#{database}.unknown')"
    )
  ensure
    ActiveRecord::Base.establish_connection(:default)
    ActiveRecord::Base.connection.execute("DROP DATABASE IF EXISTS #{other}")
    ActiveRecord::Base.connection.execute('DROP VIEW IF EXISTS structure_spec_view')
    ActiveRecord::Base.connection.execute('DROP DICTIONARY IF EXISTS `structure-spec-dict`')
  end

  it 'loads while the server already has its user-defined functions' do
    connection.execute('CREATE FUNCTION structure_spec_double AS (x) -> x * 2', nil, format: nil)

    structure = dump

    expect(structure).to include('CREATE OR REPLACE FUNCTION structure_spec_double')
    expect { load_structure(structure) }.not_to raise_error
  end

  it 'leaves out the tables that the schema dumper ignores' do
    ignore_tables = ActiveRecord::SchemaDumper.ignore_tables
    connection.execute('CREATE TABLE structure_spec (id UInt64) ENGINE = MergeTree ORDER BY id')
    connection.execute('CREATE TABLE structure_spec_ignored (id UInt64) ENGINE = MergeTree ORDER BY id')
    ActiveRecord::SchemaDumper.ignore_tables = [/ignored/]

    structure = dump

    expect(structure).to match(/CREATE TABLE structure_spec\s/)
    expect(structure).not_to include('structure_spec_ignored')
  ensure
    ActiveRecord::SchemaDumper.ignore_tables = ignore_tables
  end
end

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
    dumper.statements.map { |sql| sql[/\ACREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+VIEW|VIEW|DICTIONARY|TABLE|FUNCTION)\s+(\S+)/i, 1] }
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

  it 'puts the {database} macro back in replicated paths, so the file loads into any database' do
    # ClickHouse expands {database} when it creates the table and SHOW CREATE
    # returns the expanded path, which would tie the dump to this database.
    expanded = instance_double(ActiveRecord::ConnectionAdapters::ClickhouseAdapter, functions: [])
    allow(expanded).to receive(:quote) { |value| "'#{value}'" }
    allow(expanded).to receive(:execute).and_return({ 'data' => [['events', [], [], [], []]] })
    allow(expanded).to receive(:show_create_table).and_return(<<~SQL)
      CREATE TABLE events (id UInt64)
      ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/#{database}/events', '{replica}')
      ORDER BY id
    SQL

    sql = described_class.new(expanded, database).statements.first

    expect(sql).to include("ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/events', '{replica}')")
  end

  it 'dumps every object even when the dependency graph has a cycle' do
    # ClickHouse rejects a genuine cycle, but a column carrying the name of a
    # table can still close one in the graph.
    cyclic = instance_double(ActiveRecord::ConnectionAdapters::ClickhouseAdapter, functions: [])
    allow(cyclic).to receive(:quote) { |value| "'#{value}'" }
    allow(cyclic).to receive(:execute).and_return({ 'data' => [['a_view', [], [], [], []], ['b_view', [], [], [], []]] })
    allow(cyclic).to receive(:show_create_table) do |name, **|
      other = name == 'a_view' ? 'b_view' : 'a_view'
      "CREATE VIEW #{name} AS SELECT * FROM #{other}"
    end

    names = Timeout.timeout(10) { dumped_names(described_class.new(cyclic, database)) }

    expect(names).to contain_exactly('a_view', 'b_view')
  end
end
