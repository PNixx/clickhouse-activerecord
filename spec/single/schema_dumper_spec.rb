# frozen_string_literal: true

require 'clickhouse-activerecord/schema_dumper'

RSpec.describe ClickhouseActiverecord::SchemaDumper, :migrations do
  let(:directory) { raise 'NotImplemented' }
  let(:migrations_dir) { File.join(FIXTURES_PATH, 'migrations', directory) }
  let(:migration_context) { ActiveRecord::MigrationContext.new(migrations_dir) }

  before do
    quietly { migration_context.up }
  end

  subject do
    ClickhouseActiverecord::SchemaDumper.dump
  end

  describe ".dump" do
    context 'aggregate_function' do
      let(:directory) { 'schema_table_with_aggregate_function_creation' }

      it 'creates a table with aggregate function column for an Int32' do
        expect { subject }.to output(
          satisfy do |schema|
            expect(schema).to match(/t\.float "col1"/)
            expect(schema).to match(/"col1"[^\n]+aggregate_function: "sum"/)
            expect(schema).to match(/"col1"[^\n]+limit: 4/)
          end
        ).to_stdout_from_any_process
      end

      it 'creates a table with aggregate function column for an Int64' do
        expect { subject }.to output(
          satisfy do |schema|
            expect(schema).to match(/t\.float "col2"/)
            expect(schema).to match(/"col2"[^\n]+aggregate_function: "anyLast"/)
            expect(schema).to match(/"col2"[^\n]+limit: 8/)
          end
        ).to_stdout_from_any_process
      end

      it 'creates a table with aggregate function column for an DateTime64' do
        expect { subject }.to output(
          satisfy do |schema|
            expect(schema).to match(/t\.datetime "col3"/)
            expect(schema).to match(/"col3"[^\n]+aggregate_function: "anyLast"/)
            expect(schema).to match(/"col3"[^\n]+precision: 3/)
          end
        ).to_stdout_from_any_process
      end

      it 'creates a table with simple aggregate function column for an DateTime64' do
        expect { subject }.to output(
          satisfy do |schema|
            expect(schema).to match(/t\.datetime "col3"/)
            expect(schema).to match(/"col3"[^\n]+aggregate_function: "anyLast"/)
            expect(schema).to match(/"col3"[^\n]+precision: 3/)
            expect(schema).to match(/"t\.datetime "col4"[^\n]+simple_aggregate_function: "anyLast"/)
          end
        ).to_stdout_from_any_process
      end
    end
  end
end
