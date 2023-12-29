# frozen_string_literal: true

if ActiveRecord.version >= Gem::Version.new('6.1')
  RSpec.describe 'multi-database compatibility', :migrations do
    let(:model) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'events'
      end
    end

    let(:in_mem_model) do
      Class.new(InMemBase) do
        self.table_name = 'some'
      end
    end

    describe 'model methods' do
      describe '::is_view' do
        context 'for non-Clickhouse models' do
          it 'is false' do
            expect(in_mem_model.is_view).to be_falsey
          end

          it 'is not allowed to be set' do
            expect { in_mem_model.is_view = true }.to raise_error(NotImplementedError)
          end
        end

        context 'for Clickhouse models' do
          it 'is false' do
            expect(model.is_view).to be_falsey
          end

          context 'when set' do
            it 'returns the set value' do
              model.is_view = true
              expect(model.is_view).to be_truthy
            end
          end
        end
      end

      describe '::settings' do
        context 'for non-Clickhouse models' do
          it 'is not allowed' do
            expect { in_mem_model.settings(foo: 'bar') }.to raise_error(ActiveRecord::ActiveRecordError)
          end
        end

        context 'for Clickhouse models' do
          it 'works' do
            query = model.settings(foo: 'bar')
            expect(query.settings_values).to eq({ foo: 'bar' })
          end
        end
      end
    end

    describe 'internal metadata' do
      it 'creates ar_internal_metadata correctly for Clickhouse' do
        migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
        quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).migrate }

        expect(model.connection.table_options(:ar_internal_metadata)).to include(options: /^ReplacingMergeTree/)
      end

      it 'creates ar_internal_metadata correctly for other databases' do
        migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_decimal_creation')
        quietly { ActiveRecord::MigrationContext.new(migrations_dir, in_mem_model.connection.schema_migration).migrate }

        expect(in_mem_model.connection.table_options(:ar_internal_metadata)).to be_blank
      end
    end

    describe 'schema migrations' do
      it 'creates schema_migrations correctly for Clickhouse' do
        migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
        quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).migrate }

        expect(model.connection.table_options(:schema_migrations)).to include(options: /^ReplacingMergeTree/)
      end

      it 'creates schema_migrations correctly for other databases' do
        migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_decimal_creation')
        quietly { ActiveRecord::MigrationContext.new(migrations_dir, in_mem_model.connection.schema_migration).migrate }

        expect(in_mem_model.connection.table_options(:schema_migrations)).to be_blank
      end
    end
  end
end
