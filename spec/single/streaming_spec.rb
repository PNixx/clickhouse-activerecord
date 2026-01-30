# frozen_string_literal: true

RSpec.describe 'Streaming', :migrations do
  class Model < ActiveRecord::Base
    self.table_name = 'sample'
  end

  describe 'sample' do
    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
    end

    it 'simple' do
      path = Model.connection.execute_streaming('SELECT count(*) AS count FROM sample')
      expect(path.is_a?(String)).to be_truthy
      expect(File.read(path)).to eq("[\"count\"]\n[\"UInt64\"]\n[\"0\"]\n")
    end

    it 'JSONCompact format' do
      path = Model.connection.execute_streaming('SELECT count(*) AS count FROM sample', format: 'JSONCompact')
      data = JSON.parse(File.read(path))
      expect(data['data'][0][0]).to eq('0')
    end

    it 'JSONEachRow format' do
      path = Model.connection.execute_streaming('SELECT count(*) AS count FROM sample', format: 'JSONEachRow')
      data = JSON.parse(File.read(path))
      expect(data['count']).to eq('0')
    end

    it 'multiple rows JSONEachRow format' do
      path = Model.connection.execute_streaming('SELECT * FROM generate_series(1, 1000000)', format: 'JSONEachRow')
      lines = File.readlines(path)
      expect(JSON.parse(lines[0])).to eq('generate_series' => '1')
      expect(lines.size).to eq(1000000)
    end

    it 'multiple rows CSVWithNames format' do
      path = Model.connection.execute_streaming('SELECT * FROM generate_series(1, 1000000)', format: 'CSVWithNames')
      lines = File.readlines(path)
      expect(JSON.parse(lines[0])).to eq('generate_series')
      expect(JSON.parse(lines[1])).to eq(1)
      expect(lines.size).to eq(1000001)
    end

    it 'error' do
      expect { Model.connection.execute_streaming('error request') }.to raise_error(ActiveRecord::ActiveRecordError, include('DB::Exception'))
    end
  end
end
