# frozen_string_literal: true

RSpec.describe 'HideCredentials' do
  let(:connection) { ActiveRecord::Base.connection }

  describe '#resolve_hidden_credentials' do
    before do
      @original_resolver = ClickhouseActiverecord.mysql_credential_resolver
      ClickhouseActiverecord.mysql_credential_resolver = ->(table_name) {
        "MySQL('host:3306','test_db','#{table_name}','user','pass')"
      }
    end

    after do
      ClickhouseActiverecord.mysql_credential_resolver = @original_resolver
    end

    it 'replaces [HIDDEN] with actual credentials via resolver' do
      sql = "ENGINE = MySQL('[HIDDEN]', '[HIDDEN]', 'examples', '[HIDDEN]', '[HIDDEN]')"
      result = connection.send(:resolve_hidden_credentials, sql)
      expect(result).to eq("ENGINE = MySQL('host:3306','test_db','examples','user','pass')")
    end

    it 'returns sql unchanged if no [HIDDEN]' do
      sql = "CREATE TABLE test (id UInt64) ENGINE = MergeTree ORDER BY id"
      result = connection.send(:resolve_hidden_credentials, sql)
      expect(result).to eq(sql)
    end

    it 'works without a resolver set' do
      ClickhouseActiverecord.mysql_credential_resolver = nil
      sql = "ENGINE = MySQL('[HIDDEN]', '[HIDDEN]', 'examples', '[HIDDEN]', '[HIDDEN]')"
      result = connection.send(:resolve_hidden_credentials, sql)
      expect(result).to eq(sql)
    end

    it 'passes the correct table_name to the resolver' do
      passed_tables = []
      ClickhouseActiverecord.mysql_credential_resolver = ->(table_name) {
        passed_tables << table_name
        "MySQL('h:1','db','#{table_name}','u','p')"
      }
      connection.send(:resolve_hidden_credentials,
        "ENGINE = MySQL('[HIDDEN]', '[HIDDEN]', 'issues', '[HIDDEN]', '[HIDDEN]')")
      expect(passed_tables).to eq(['issues'])
    end

    it 'handles multiple MySQL engine references' do
      sql = "ENGINE = MySQL('[HIDDEN]','[HIDDEN]','t1','[HIDDEN]','[HIDDEN]'), " \
            "ENGINE = MySQL('[HIDDEN]','[HIDDEN]','t2','[HIDDEN]','[HIDDEN]')"
      result = connection.send(:resolve_hidden_credentials, sql)
      expect(result).to include("MySQL('host:3306','test_db','t1','user','pass')")
      expect(result).to include("MySQL('host:3306','test_db','t2','user','pass')")
    end
  end

  describe '#raw_execute resolves hidden credentials' do
    around do |example|
      ClickhouseActiverecord.mysql_credential_resolver = ->(table_name) {
        "MySQL('host:3306','test_db','#{table_name}','user','pass')"
      }
      example.run
      ClickhouseActiverecord.mysql_credential_resolver = nil
    end

    it 'calls resolve_hidden_credentials before executing sql' do
      expect(connection).to receive(:resolve_hidden_credentials).with("SELECT 1").and_call_original
      connection.execute("SELECT 1")
    end
  end

  describe 'SchemaDumper#format_options' do
    let(:dumper) { ClickhouseActiverecord::SchemaDumper.new(connection) }

    it 'replaces MySQL credentials with [HIDDEN]' do
      options = { options: "MySQL('host:3306','test_db','examples','user','pass') SETTINGS connection_pool_size=8" }
      result = dumper.send(:format_options, options)
      expect(result).to include("MySQL('[HIDDEN]', '[HIDDEN]', 'examples', '[HIDDEN]', '[HIDDEN]')")
      expect(result).not_to include('host:3306')
      expect(result).not_to include('test_db')
      expect(result).not_to include('user')
      expect(result).not_to include('pass')
    end

    it 'still handles Replicated engine URLs' do
      options = { options: "ReplicatedMergeTree('/clickhouse/tables/1/test','{replica}') PARTITION BY toDate(date) ORDER BY id" }
      result = dumper.send(:format_options, options)
      expect(result).to include("MergeTree")
      expect(result).not_to include("ReplicatedMergeTree")
    end

    it 'handles both Replicated and MySQL in same options' do
      options = { options: "ReplicatedMergeTree('/clickhouse/tables/1/test','{replica}') ORDER BY id" }
      options[:options] += " SETTINGS connection_pool_size=8"
      result = dumper.send(:format_options, options)
      expect(result).not_to include("ReplicatedMergeTree")
    end

    it 'returns nil options unchanged' do
      result = dumper.send(:format_options, nil)
      expect(result).to be_nil
    end

    it 'returns options without :options key unchanged' do
      result = dumper.send(:format_options, { as: "SELECT 1" })
      expect(result).to eq({ as: "SELECT 1" }.to_s)
    end
  end

  describe 'SchemaDumper#dictionary_names' do
    let(:dumper) { ClickhouseActiverecord::SchemaDumper.new(connection) }

    it 'returns an array' do
      names = dumper.send(:dictionary_names)
      expect(names).to be_an(Array)
    end
  end

  describe 'SchemaDumper#tables skips dictionary tables' do
    let(:stream) { StringIO.new }
    let(:dumper) { ClickhouseActiverecord::SchemaDumper.new(connection) }

    after do
      connection.execute("DROP TABLE IF EXISTS dict_test_table")
      connection.execute("DROP DICTIONARY IF EXISTS test_dict")
    end

    it 'excludes dictionary engine tables' do
      connection.execute("CREATE TABLE dict_test_table (id UInt64, name String) ENGINE = MergeTree ORDER BY id")
      connection.execute(<<~SQL)
        CREATE DICTIONARY test_dict (
          id UInt64,
          name String
        )
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'dict_test_table'))
        LIFETIME(MIN 0 MAX 0)
        LAYOUT(FLAT())
      SQL

      dumper.send(:tables, stream)
      output = stream.string

      expect(output).to include("dict_test_table")
      expect(output).not_to include("test_dict")
    end
  end
end
