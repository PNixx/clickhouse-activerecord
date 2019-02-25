# frozen_string_literal: true

RSpec.describe 'Migration', :migrations do
  describe 'performs migrations' do
    let(:model) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'some'
      end
    end

    context 'table creation' do
      context 'plain' do
        it 'creates a table' do
          migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'plain_table_creation')
          quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }

          current_schema = schema(model)

          expect(current_schema.keys.count).to eq(2)
          expect(current_schema).to have_key('id')
          expect(current_schema).to have_key('date')
          expect(current_schema['id'].sql_type).to eq('UInt64')
          expect(current_schema['date'].sql_type).to eq('Date')
        end
      end

      context 'dsl' do
        context 'empty' do
          it 'creates a table' do
            migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_creation')
            quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }

            current_schema = schema(model)

            expect(current_schema.keys.count).to eq(1)
            expect(current_schema).to have_key('id')
            expect(current_schema['id'].sql_type).to eq('UInt32')
          end
        end

        context 'with engine' do
          it 'creates a table' do
            migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_engine_creation')
            quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }

            current_schema = schema(model)

            expect(current_schema.keys.count).to eq(2)
            expect(current_schema).to have_key('id')
            expect(current_schema).to have_key('date')
            expect(current_schema['id'].sql_type).to eq('UInt32')
            expect(current_schema['date'].sql_type).to eq('Date')
          end
        end

        context 'types' do
          context 'decimal' do
            it 'creates a table with valid scale and precision' do
              migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_decimal_creation')
              quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(3)
              expect(current_schema).to have_key('id')
              expect(current_schema).to have_key('money')
              expect(current_schema).to have_key('balance')
              expect(current_schema['id'].sql_type).to eq('UInt32')
              expect(current_schema['money'].sql_type).to eq('Nullable(Decimal(16, 4))')
              expect(current_schema['balance'].sql_type).to eq('Decimal(32, 2)')
            end
          end
        end
      end
    end

    describe 'drop table' do
      it 'drops table' do
        migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_drop_table')
        quietly { ActiveRecord::MigrationContext.new(migrations_dir).up(1) }

        expect(ActiveRecord::Base.connection.tables).to include('some')

        quietly { ActiveRecord::MigrationContext.new(migrations_dir).up(2) }

        expect(ActiveRecord::Base.connection.tables).not_to include('some')
      end
    end

    describe 'add column' do
      it 'adds a new column' do
        migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_add_column')
        quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }

        current_schema = schema(model)

        expect(current_schema.keys.count).to eq(3)
        expect(current_schema).to have_key('id')
        expect(current_schema).to have_key('date')
        expect(current_schema).to have_key('new_column')
        expect(current_schema['id'].sql_type).to eq('UInt32')
        expect(current_schema['date'].sql_type).to eq('Date')
        expect(current_schema['new_column'].sql_type).to eq('Nullable(UInt64)')
      end
    end

    describe 'drop column' do
      it 'drops column' do
        migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_drop_column')
        quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }

        current_schema = schema(model)

        expect(current_schema.keys.count).to eq(1)
        expect(current_schema).to have_key('date')
        expect(current_schema['date'].sql_type).to eq('Date')
      end
    end
  end
end
