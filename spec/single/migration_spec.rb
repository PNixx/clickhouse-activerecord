# frozen_string_literal: true

RSpec.describe 'Migration', :migrations do
  describe 'performs migrations' do
    let(:model) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'some'
      end
    end
    let(:directory) { raise 'NotImplemented' }
    let(:migrations_dir) { File.join(FIXTURES_PATH, 'migrations', directory) }
    let(:migration_context) { ActiveRecord::MigrationContext.new(migrations_dir) }

    connection_config = ActiveRecord::Base.connection_db_config.configuration_hash

    subject do
      quietly { migration_context.up }
    end

    context 'database creation' do
      let(:db) { (0...8).map { (65 + rand(26)).chr }.join.downcase }

      it 'create' do
        model.connection.create_database(db)
      end

      after { model.connection.drop_database(db) }
    end

    context 'table creation' do
      context 'plain' do
        let(:directory) { 'plain_table_creation' }

        it 'creates a table' do
          subject

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
          let(:directory) { 'dsl_table_creation' }
          it 'creates a table' do
            subject

            current_schema = schema(model)

            expect(current_schema.keys.count).to eq(1)
            expect(current_schema).to have_key('id')
            expect(current_schema['id'].sql_type).to eq('UInt32')
          end
        end

        context 'without id' do
          let(:directory) { 'dsl_create_view_without_id' }
          it 'creates a table' do
            subject

            current_schema = schema(model)

            expect(current_schema.keys.count).to eq(1)
            expect(current_schema).to_not have_key('id')
            expect(current_schema['col'].sql_type).to eq('String')
          end
        end

        context 'with buffer table' do
          let(:directory) { 'dsl_table_buffer_creation' }
          it 'creates a table' do
            subject

            expect(ActiveRecord::Base.connection.tables).to include('some_buffers')
          end
        end

        context 'with engine' do
          let(:directory) { 'dsl_table_with_engine_creation' }
          it 'creates a table' do
            subject

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
            let(:directory) { 'dsl_table_with_decimal_creation' }
            it 'creates a table with valid scale and precision' do
              subject

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(4)
              expect(current_schema).to have_key('id')
              expect(current_schema).to have_key('money')
              expect(current_schema).to have_key('balance')
              expect(current_schema['id'].sql_type).to eq('UInt32')
              expect(current_schema['money'].sql_type).to eq('Nullable(Decimal(16, 4))')
              expect(current_schema['balance'].sql_type).to eq('Decimal(32, 2)')
              expect(current_schema['balance'].default).to eq(0.0)
              expect(current_schema['paid'].default).to eq(1.15)
            end
          end

          context 'uuid' do
            let(:directory) { 'dsl_table_with_uuid_creation' }
            it 'creates a table with uuid columns' do
              subject

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(2)
              expect(current_schema).to have_key('col1')
              expect(current_schema).to have_key('col2')
              expect(current_schema['col1'].sql_type).to eq('UUID')
              expect(current_schema['col2'].sql_type).to eq('Nullable(UUID)')
            end
          end

          context 'datetime' do
            let(:directory) { 'dsl_table_with_datetime_creation' }
            it 'creates a table with datetime columns' do
              subject

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(2)
              expect(current_schema).to have_key('datetime')
              expect(current_schema).to have_key('datetime64')
              expect(current_schema['datetime'].sql_type).to eq('DateTime')
              expect(current_schema['datetime'].default).to be_nil
              expect(current_schema['datetime'].default_function).to eq('now()')
              expect(current_schema['datetime64'].sql_type).to eq('Nullable(DateTime64(3))')
              expect(current_schema['datetime64'].default).to be_nil
              expect(current_schema['datetime64'].default_function).to eq('now64()')
            end
          end

          context 'low_cardinality' do
            let(:directory) { 'dsl_table_with_low_cardinality_creation' }
            it 'creates a table with low cardinality columns' do
              subject

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(4)
              expect(current_schema).to have_key('col1')
              expect(current_schema).to have_key('col2')
              expect(current_schema).to have_key('col3')
              expect(current_schema).to have_key('col4')
              expect(current_schema['col1'].sql_type).to eq('LowCardinality(String)')
              expect(current_schema['col1'].default).to eq('col')
              expect(current_schema['col2'].sql_type).to eq('LowCardinality(Nullable(String))')
              expect(current_schema['col3'].sql_type).to eq('Array(LowCardinality(Nullable(String)))')
              expect(current_schema['col4'].sql_type).to eq('Map(String, LowCardinality(Nullable(String)))')
            end
          end

          context 'fixed_string' do
            let(:directory) { 'dsl_table_with_fixed_string_creation' }
            it 'creates a table with fixed string columns' do
              subject

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(3)
              expect(current_schema).to have_key('fixed_string1')
              expect(current_schema).to have_key('fixed_string16_array')
              expect(current_schema).to have_key('fixed_string16_map')
              expect(current_schema['fixed_string1'].sql_type).to eq('FixedString(1)')
              expect(current_schema['fixed_string16_array'].sql_type).to eq('Array(Nullable(FixedString(16)))')
              expect(current_schema['fixed_string16_map'].sql_type).to eq('Map(String, Nullable(FixedString(16)))')
            end
          end

          context 'enum' do
            let(:directory) { 'dsl_table_with_enum_creation' }
            it 'creates a table with enum columns' do
              subject

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(3)
              expect(current_schema).to have_key('enum8')
              expect(current_schema).to have_key('enum16')
              expect(current_schema).to have_key('enum_nullable')
              expect(current_schema['enum8'].sql_type).to eq("Enum8('key1' = 1, 'key2' = 2)")
              expect(current_schema['enum8'].default).to eq('key1')
              expect(current_schema['enum16'].sql_type).to eq("Enum16('key1' = 1, 'key2' = 2)")
              expect(current_schema['enum_nullable'].sql_type).to eq("Nullable(Enum8('key1' = 1, 'key2' = 2))")
            end
          end
        end

        context 'no database' do
          before(:all) do
            ActiveRecord::Base.establish_connection(connection_config.merge(database: 'test_not_exist'))
          end

          after(:all) do
            ActiveRecord::Base.establish_connection(connection_config)
          end

          let(:directory) { 'plain_table_creation' }
          it 'raise error' do
            expect { subject }.to raise_error(ActiveRecord::NoDatabaseError)
          end
        end

        context 'creates a view' do
          let(:directory) { 'dsl_create_view_with_to_section' }
          it 'creates a view' do
            subject

            expect(ActiveRecord::Base.connection.tables).to include('some_view')
          end
        end

        context 'drops a view' do
          let(:directory) { 'dsl_create_view_without_to_section' }
          it 'drops a view' do
            subject

            expect(ActiveRecord::Base.connection.tables).to include('some_view')

            quietly do
              migration_context.down
            end

            expect(ActiveRecord::Base.connection.tables).not_to include('some_view')
          end
        end

        context 'with index' do
          let(:directory) { 'dsl_create_table_with_index' }

          it 'creates a table' do
            quietly { migration_context.up(1) }

            expect(ActiveRecord::Base.connection.show_create_table('some')).to include('INDEX idx (int1 * int2, date) TYPE minmax GRANULARITY 3')

            quietly { migration_context.up(2) }

            expect(ActiveRecord::Base.connection.show_create_table('some')).to_not include('INDEX idx')

            quietly { migration_context.up(3) }

            expect(ActiveRecord::Base.connection.show_create_table('some')).to include('INDEX idx2 int1 * int2 TYPE set(10) GRANULARITY 4')
          end

          it 'add index if not exists' do
            subject

            expect { ActiveRecord::Base.connection.add_index('some', 'int1 + int2', name: 'idx2', type: 'minmax', granularity: 1) }.to raise_error(ActiveRecord::ActiveRecordError, include('already exists'))

            ActiveRecord::Base.connection.add_index('some', 'int1 + int2', name: 'idx2', type: 'minmax', granularity: 1, if_not_exists: true)
          end

          it 'drop index if exists' do
            subject

            expect { ActiveRecord::Base.connection.remove_index('some', 'idx3') }.to raise_error(ActiveRecord::ActiveRecordError, include('Cannot find index'))

            ActiveRecord::Base.connection.remove_index('some', 'idx2')
          end

          it 'rebuid index' do
            subject

            expect { ActiveRecord::Base.connection.rebuild_index('some', 'idx3') }.to raise_error(ActiveRecord::ActiveRecordError, include('Unknown index'))

            # expect { ActiveRecord::Base.connection.rebuild_index('some', 'idx3', if_exists: true) }.to_not raise_error

            ActiveRecord::Base.connection.rebuild_index('some', 'idx2')
          end

          it 'clear index' do
            subject

            ActiveRecord::Base.connection.clear_index('some', 'idx2')
          end
        end
      end
    end

    describe 'drop table' do
      let(:directory) { 'dsl_drop_table' }
      it 'drops table' do
        quietly { migration_context.up(1) }

        expect(ActiveRecord::Base.connection.tables).to include('some')

        quietly { migration_context.up(2) }

        expect(ActiveRecord::Base.connection.tables).not_to include('some')
      end
    end

    describe 'drop table sync' do
      it 'drops table' do
        migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_drop_table_sync')
        quietly { ActiveRecord::MigrationContext.new(migrations_dir).up(1) }

        expect(ActiveRecord::Base.connection.tables).to include('some')

        quietly { ActiveRecord::MigrationContext.new(migrations_dir).up(2) }

        expect(ActiveRecord::Base.connection.tables).not_to include('some')
      end
    end

    describe 'add column' do
      let(:directory) { 'dsl_add_column' }
      it 'adds a new column' do
        subject

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
      let(:directory) { 'dsl_drop_column' }
      it 'drops column' do
        subject

        current_schema = schema(model)

        expect(current_schema.keys.count).to eq(1)
        expect(current_schema).to have_key('date')
        expect(current_schema['date'].sql_type).to eq('Date')
      end
    end

    context 'function creation' do
      after do
        ActiveRecord::Base.connection.drop_functions
      end

      context 'plain' do
        let(:directory) { 'plain_function_creation' }
        it 'creates a function' do
          subject

          expect(ActiveRecord::Base.connection.functions).to match_array(['some_fun'])
        end
      end

      context 'dsl' do
        let(:directory) { 'dsl_create_function' }
        it 'creates a function' do
          subject

          expect(ActiveRecord::Base.connection.functions).to match_array(['some_fun'])
        end
      end
    end
  end
end
