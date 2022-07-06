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
          quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

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
            quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

            current_schema = schema(model)

            expect(current_schema.keys.count).to eq(1)
            expect(current_schema).to have_key('id')
            expect(current_schema['id'].sql_type).to eq('UInt32')
          end
        end

        context 'with engine' do
          it 'creates a table' do
            migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_engine_creation')
            quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

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
              quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

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

          context 'uuid' do
            it 'creates a table with uuid columns' do
              migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_uuid_creation')
              quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(2)
              expect(current_schema).to have_key('col1')
              expect(current_schema).to have_key('col2')
              expect(current_schema['col1'].sql_type).to eq('UUID')
              expect(current_schema['col2'].sql_type).to eq('Nullable(UUID)')
            end
          end

          context 'datetime' do
            it 'creates a table with datetime columns' do
              migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_datetime_creation')
              quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(2)
              expect(current_schema).to have_key('datetime')
              expect(current_schema).to have_key('datetime64')
              expect(current_schema['datetime'].sql_type).to eq('DateTime')
              expect(current_schema['datetime64'].sql_type).to eq('Nullable(DateTime64(3))')
            end
          end

          context 'low_cardinality' do
            it 'creates a table with low cardinality columns' do
              migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_low_cardinality_creation')
              quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(3)
              expect(current_schema).to have_key('col1')
              expect(current_schema).to have_key('col2')
              expect(current_schema).to have_key('col3')
              expect(current_schema['col1'].sql_type).to eq('LowCardinality(String)')
              expect(current_schema['col2'].sql_type).to eq('LowCardinality(Nullable(String))')
              expect(current_schema['col3'].sql_type).to eq('Array(LowCardinality(Nullable(String)))')
            end
          end

          context 'fixed_string' do
            it 'creates a table with fixed string columns' do
              migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_fixed_string_creation')
              quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(2)
              expect(current_schema).to have_key('fixed_string1')
              expect(current_schema).to have_key('fixed_string16_array')
              expect(current_schema['fixed_string1'].sql_type).to eq('FixedString(1)')
              expect(current_schema['fixed_string16_array'].sql_type).to eq('Array(Nullable(FixedString(16)))')
            end
          end

          context 'enum' do
            it 'creates a table with enum columns' do
              migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_table_with_enum_creation')
              quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(3)
              expect(current_schema).to have_key('enum8')
              expect(current_schema).to have_key('enum16')
              expect(current_schema).to have_key('enum_nullable')
              expect(current_schema['enum8'].sql_type).to eq("Enum8('key1' = 1, 'key2' = 2)")
              expect(current_schema['enum16'].sql_type).to eq("Enum16('key1' = 1, 'key2' = 2)")
              expect(current_schema['enum_nullable'].sql_type).to eq("Nullable(Enum8('key1' = 1, 'key2' = 2))")
            end
          end
        end

        context 'with distributed' do
          let(:model_distributed) do
            Class.new(ActiveRecord::Base) do
              self.table_name = 'some_distributed'
            end
          end
          connection_config = ActiveRecord::Base.connection_db_config.configuration_hash

          before(:all) do
            ActiveRecord::Base.establish_connection(connection_config.merge(cluster_name: CLUSTER_NAME))
          end

          after(:all) do
            ActiveRecord::Base.establish_connection(connection_config)
          end

          it 'creates a table with distributed table' do
            migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_create_table_with_distributed')
            quietly { ActiveRecord::MigrationContext.new(migrations_dir, ClickhouseActiverecord::SchemaMigration).up }

            current_schema = schema(model)
            current_schema_distributed = schema(model_distributed)

            expect(current_schema.keys.count).to eq(1)
            expect(current_schema_distributed.keys.count).to eq(1)

            expect(current_schema).to have_key('date')
            expect(current_schema_distributed).to have_key('date')

            expect(current_schema['date'].sql_type).to eq('Date')
            expect(current_schema_distributed['date'].sql_type).to eq('Date')
          end

          it 'drops a table with distributed table' do
            migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_create_table_with_distributed')
            quietly { ActiveRecord::MigrationContext.new(migrations_dir, ClickhouseActiverecord::SchemaMigration).up }

            expect(ActiveRecord::Base.connection.tables).to include('some')
            expect(ActiveRecord::Base.connection.tables).to include('some_distributed')

            quietly do
              ClickhouseActiverecord::MigrationContext.new(migrations_dir, ClickhouseActiverecord::SchemaMigration).down
            end

            expect(ActiveRecord::Base.connection.tables).not_to include('some')
            expect(ActiveRecord::Base.connection.tables).not_to include('some_distributed')
          end
        end

        context 'view' do
          it 'creates a view' do
            migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_create_view_with_to_section')
            quietly { ActiveRecord::MigrationContext.new(migrations_dir, ClickhouseActiverecord::SchemaMigration).up }

            expect(ActiveRecord::Base.connection.tables).to include('some_view')
          end

          it 'drops a view' do
            migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_create_view_without_to_section')
            quietly { ActiveRecord::MigrationContext.new(migrations_dir, ClickhouseActiverecord::SchemaMigration).up }

            expect(ActiveRecord::Base.connection.tables).to include('some_view')
            expect(ActiveRecord::Base.connection.tables).to include('.inner.some_view')

            quietly do
              ClickhouseActiverecord::MigrationContext.new(migrations_dir, ClickhouseActiverecord::SchemaMigration).down
            end

            expect(ActiveRecord::Base.connection.tables).not_to include('some_view')
            expect(ActiveRecord::Base.connection.tables).not_to include('.inner.some_view')
          end
        end

        context 'dictionary' do
          it 'creates dictionary with table' do
            migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_create_dictionary_with_table')
            quietly { ActiveRecord::MigrationContext.new(migrations_dir, ClickhouseActiverecord::SchemaMigration).up }

            expect(ActiveRecord::Base.connection.tables).to include('some_dictionary')
            expect(ActiveRecord::Base.connection.tables).to include('some_table')
          end

          it 'drops a dictionary with a table' do
            migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_create_dictionary_with_table')
            quietly { ActiveRecord::MigrationContext.new(migrations_dir, ClickhouseActiverecord::SchemaMigration).up }

            expect(ActiveRecord::Base.connection.tables).to include('some_dictionary')
            expect(ActiveRecord::Base.connection.tables).to include('some_table')

            quietly do
              ClickhouseActiverecord::MigrationContext.new(migrations_dir, ClickhouseActiverecord::SchemaMigration).down
            end

            expect(ActiveRecord::Base.connection.tables).not_to include('some_dictionary')
            expect(ActiveRecord::Base.connection.tables).not_to include('some_table')
          end
        end
      end
    end

    describe 'drop table' do
      it 'drops table' do
        migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_drop_table')
        quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up(1) }

        expect(ActiveRecord::Base.connection.tables).to include('some')

        quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up(2) }

        expect(ActiveRecord::Base.connection.tables).not_to include('some')
      end
    end

    describe 'add column' do
      it 'adds a new column' do
        migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'dsl_add_column')
        quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

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
        quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

        current_schema = schema(model)

        expect(current_schema.keys.count).to eq(1)
        expect(current_schema).to have_key('date')
        expect(current_schema['date'].sql_type).to eq('Date')
      end
    end
  end
end
