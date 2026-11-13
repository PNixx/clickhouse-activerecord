# frozen_string_literal: true

require 'tmpdir'

RSpec.describe 'Schema cache fill' do
  let(:connection) { ActiveRecord::Base.connection }

  # One table of every kind the schema cache has to describe
  let(:ddl) do
    {
      sc_composite_key: <<~SQL,
        CREATE TABLE sc_composite_key (
          b UInt32,
          a UInt32,
          note String COMMENT 'a comment',
          packed String CODEC(ZSTD(1)),
          chained UInt64 CODEC(Delta, ZSTD),
          aliased UInt32 ALIAS a + 1,
          stored UInt32 MATERIALIZED a * 2,
          input UInt32 EPHEMERAL,
          with_default UInt32 DEFAULT 5,
          nullable Nullable(String)
        ) ENGINE = MergeTree ORDER BY (b, a)
      SQL
      sc_single_key: 'CREATE TABLE sc_single_key (id UInt64, name String) ENGINE = MergeTree ORDER BY id',
      sc_no_key: 'CREATE TABLE sc_no_key (x UInt8) ENGINE = MergeTree ORDER BY tuple()',
      sc_primary_key_prefix: <<~SQL,
        CREATE TABLE sc_primary_key_prefix (id UInt64, ts DateTime) ENGINE = MergeTree PRIMARY KEY id ORDER BY (id, ts)
      SQL
      sc_replacing: 'CREATE TABLE sc_replacing (id UInt64, v String) ENGINE = ReplacingMergeTree ORDER BY id',
      sc_log: 'CREATE TABLE sc_log (id UInt64) ENGINE = Log',
      sc_view: 'CREATE VIEW sc_view AS SELECT b, a FROM sc_composite_key',
      # Without TO, ClickHouse creates a hidden `.inner_id.<uuid>` table which must stay out of the cache
      sc_materialized_view: <<~SQL,
        CREATE MATERIALIZED VIEW sc_materialized_view ENGINE = MergeTree ORDER BY b AS SELECT b, a FROM sc_composite_key
      SQL
      sc_dictionary: <<~SQL,
        CREATE DICTIONARY sc_dictionary (k UInt64, val String) PRIMARY KEY k SOURCE(NULL()) LIFETIME(0) LAYOUT(FLAT())
      SQL
    }
  end

  let(:tmp_dir) { Dir.mktmpdir('schema_cache_spec') }

  before do
    drop_fixtures
    ddl.each_value { |sql| connection.execute(sql) }
  end

  after do
    drop_fixtures
    connection.schema_cache.clear!
    FileUtils.remove_entry(tmp_dir)
  end

  def drop_fixtures
    connection.execute('DROP VIEW IF EXISTS sc_materialized_view SYNC')
    connection.execute('DROP VIEW IF EXISTS sc_view SYNC')
    connection.execute('DROP DICTIONARY IF EXISTS sc_dictionary SYNC')
    (ddl.keys - %i[sc_view sc_materialized_view sc_dictionary] + [:sc_late]).each do |table|
      connection.execute("DROP TABLE IF EXISTS #{table} SYNC")
    end
  end

  # Every HTTP request goes through `raw_execute`. System queries are not published as
  # `sql.active_record` events unless the `debug` option is set, so count them here instead.
  def queries_during
    queries = []
    allow(connection).to receive(:raw_execute).and_wrap_original do |original, sql, **options|
      queries << sql
      original.call(sql, **options)
    end
    yield
    queries
  end

  # Fill the whole cache the way `db:schema:cache:dump` does and return the dump with the queries it sent
  def dump_cache(name)
    path = File.join(tmp_dir, "#{name}.yml")
    # Rails hands back an already known column when a new one is `==` to it, and `==` ignores
    # `codec`. Start from an empty registry so no earlier fill (or spec) leaks into this one.
    ActiveRecord::ConnectionAdapters::Clickhouse::Column.registry.clear
    connection.schema_cache.clear!
    queries = queries_during { connection.schema_cache.dump_to(path) }
    [File.read(path), queries]
  end

  # Today's behaviour: one DESCRIBE TABLE and one SHOW COLUMNS per table
  def dump_cache_per_table(name)
    allow(connection).to receive(:with_bulk_schema_load) { |&block| block.call }
    dump_cache(name)
  end

  # What the filled cache holds for the fixture tables. The YAML dump leaves out `codec` and
  # `default_kind`, so compare the cached columns themselves as well.
  def cache_snapshot
    cache = connection.schema_cache
    ddl.keys.map(&:to_s).index_with do |table|
      {
        primary_key: cache.primary_keys(table),
        columns: cache.columns(table).map do |column|
          [column.name, column.sql_type, column.default, column.default_function, column.null,
           column.comment, column.codec, column.default_kind.to_s]
        end,
      }
    end
  end

  describe 'with the bulk load' do
    it 'dumps the same cache as one request per table' do
      bulk_dump, = dump_cache('bulk')
      bulk_snapshot = cache_snapshot
      per_table_dump, = dump_cache_per_table('per_table')
      per_table_snapshot = cache_snapshot

      expect(bulk_dump).to include('sc_composite_key')
      expect(bulk_dump).to eq(per_table_dump)
      expect(bulk_snapshot.keys.size).to eq(ddl.size)
      expect(bulk_snapshot).to eq(per_table_snapshot)
    end

    it 'reads columns and primary keys in one request' do
      _, queries = dump_cache('bulk')

      expect(queries.grep(/system\.columns/).size).to eq(1)
      expect(queries.grep(/DESCRIBE TABLE/)).to be_empty
      expect(queries.grep(/SHOW COLUMNS/)).to be_empty
    end

    it 'sends two requests per table without it' do
      _, queries = dump_cache_per_table('per_table')

      expect(queries.grep(/system\.columns/)).to be_empty
      expect(queries.grep(/DESCRIBE TABLE/).size).to be >= ddl.size
      expect(queries.grep(/SHOW COLUMNS/).size).to be >= ddl.size
    end

    it 'keeps the shape of the primary keys' do
      dump_cache('bulk')
      cache = connection.schema_cache

      expect(cache.primary_keys('sc_no_key')).to be_nil
      expect(cache.primary_keys('sc_single_key')).to eq('id')
      expect(cache.primary_keys('sc_primary_key_prefix')).to eq('id')
      expect(cache.primary_keys('sc_composite_key')).to eq(%w[a b])
      expect(cache.primary_keys('sc_view')).to be_nil
      expect(cache.primary_keys('sc_materialized_view')).to be_nil
      expect(cache.primary_keys('sc_dictionary')).to be_nil
    end

    it 'keeps alias and materialized columns, skips ephemeral ones and strips the codec wrapper' do
      dump_cache('bulk')
      columns = connection.schema_cache.columns_hash('sc_composite_key')

      expect(columns.keys).to eq(%w[b a note packed chained aliased stored with_default nullable])
      expect(columns['packed'].codec).to eq('ZSTD(1)')
      expect(columns['chained'].codec).to eq('Delta(8), ZSTD(1)')
      expect(columns['aliased'].default_kind).to be_alias
    end

    it 'leaves the hidden inner tables out' do
      bulk_dump, = dump_cache('bulk')

      expect(bulk_dump).not_to include('.inner_id.')
    end

    it 'falls back to a request for a table created after the load' do
      connection.with_bulk_schema_load do
        connection.execute('CREATE TABLE sc_late (id UInt64) ENGINE = MergeTree ORDER BY id')

        expect(connection.columns('sc_late').map(&:name)).to eq(['id'])
        expect(connection.primary_key('sc_late')).to eq('id')
      end
    end
  end

  describe 'single table lookups' do
    it 'keep sending their own requests' do
      connection.schema_cache.clear!

      queries = queries_during { connection.schema_cache.add('sc_composite_key') }

      expect(queries.grep(/DESCRIBE TABLE `sc_composite_key`/).size).to eq(1)
      expect(queries.grep(/SHOW COLUMNS FROM `sc_composite_key`/).size).to eq(1)
      expect(queries.grep(/system\.columns/)).to be_empty
    end
  end

  # Rails 7.1 passes a connection to `SchemaCache#add_all`, Rails 7.2 and later pass a pool.
  # CI does not run Rails 7.1, so drive the hook with both shapes on every version.
  describe 'the add_all hook' do
    let(:schema_cache) do
      rails_schema_cache = Class.new { def add_all(pool_or_connection) = pool_or_connection }
      Class.new(rails_schema_cache) { prepend CoreExtensions::ActiveRecord::SchemaCache }.new
    end

    it 'loads in bulk when Rails passes a connection' do
      expect(connection).to receive(:with_bulk_schema_load).and_call_original

      expect(schema_cache.add_all(connection)).to equal(connection)
    end

    it 'loads in bulk when Rails passes a pool' do
      pool = ActiveRecord::Base.connection_pool
      expect(connection).to receive(:with_bulk_schema_load).and_call_original

      expect(schema_cache.add_all(pool)).to equal(pool)
    end

    it 'leaves a connection that cannot load in bulk alone' do
      other_connection = Object.new
      pool = instance_double(ActiveRecord::ConnectionAdapters::ConnectionPool)
      allow(pool).to receive(:with_connection).and_yield(other_connection)

      expect(schema_cache.add_all(pool)).to equal(pool)
    end
  end

  describe 'on a server older than 23.4' do
    it 'keeps the request per table' do
      allow(connection).to receive(:server_version).and_return('23.3.9.55')

      _, queries = dump_cache('old_server')

      expect(queries.grep(/system\.columns/)).to be_empty
      expect(queries.grep(/DESCRIBE TABLE/).size).to be >= ddl.size
    end
  end
end
