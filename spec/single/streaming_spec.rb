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
      file = Model.connection.execute_to_file('SELECT count(*) AS count FROM sample')
      expect(file.is_a?(Tempfile)).to be_truthy
      if Model.connection.server_version.to_f < 25
        expect(file.read).to eq("[\"count\"]\n[\"UInt64\"]\n[\"0\"]\n")
      else
        expect(file.read).to eq("[\"count\"]\n[\"UInt64\"]\n[0]\n")
      end
    end

    it 'JSONCompact format' do
      file = Model.connection.execute_to_file('SELECT count(*) AS count FROM sample', format: 'JSONCompact')
      data = JSON.parse(file.read)
      if Model.connection.server_version.to_f < 25
        expect(data['data'][0][0]).to eq('0')
      else
        expect(data['data'][0][0]).to eq(0)
      end
    end

    it 'JSONEachRow format' do
      file = Model.connection.execute_to_file('SELECT count(*) AS count FROM sample', format: 'JSONEachRow')
      data = JSON.parse(file.read)
      if Model.connection.server_version.to_f < 25
        expect(data['count']).to eq('0')
      else
        expect(data['count']).to eq(0)
      end
    end

    it 'multiple rows JSONEachRow format' do
      file = Model.connection.execute_to_file('SELECT * FROM generate_series(1, 1000000)', format: 'JSONEachRow')
      lines = file.readlines
      if Model.connection.server_version.to_f < 25
        expect(JSON.parse(lines[0])).to eq('generate_series' => '1')
      else
        expect(JSON.parse(lines[0])).to eq('generate_series' => 1)
      end
      expect(lines.size).to eq(1000000)
    end

    it 'multiple rows CSVWithNames format' do
      file = Model.connection.execute_to_file('SELECT * FROM generate_series(1, 1000000)', format: 'CSVWithNames')
      lines = file.readlines
      expect(JSON.parse(lines[0])).to eq('generate_series')
      expect(JSON.parse(lines[1])).to eq(1)
      expect(lines.size).to eq(1000001)
    end

    it 'error' do
      expect { Model.connection.execute_to_file('error request') }.to raise_error(ActiveRecord::ActiveRecordError, include('DB::Exception'))
    end
  end
end
