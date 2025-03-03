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
    let(:migration_context) { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration) }

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

    describe 'table creation' do
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
              expect(current_schema['datetime64'].sql_type).to eq('Nullable(DateTime64(3))')
            end
          end

          context 'low_cardinality' do
            let(:directory) { 'dsl_table_with_low_cardinality_creation' }

            it 'creates a table with low cardinality columns' do
              subject

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
            let(:directory) { 'dsl_table_with_fixed_string_creation' }

            it 'creates a table with fixed string columns' do
              subject

              current_schema = schema(model)

              expect(current_schema.keys.count).to eq(2)
              expect(current_schema).to have_key('fixed_string1')
              expect(current_schema).to have_key('fixed_string16_array')
              expect(current_schema['fixed_string1'].sql_type).to eq('FixedString(1)')
              expect(current_schema['fixed_string16_array'].sql_type).to eq('Array(Nullable(FixedString(16)))')
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

          it 'drops a view' do
            subject

            expect(ActiveRecord::Base.connection.tables).to include('some_view')

            quietly do
              migration_context.down
            end

            expect(ActiveRecord::Base.connection.tables).not_to include('some_view')
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

      describe 'drop table sync' do
        it 'drops table' do
          quietly { migration_context.up(1) }

          expect(ActiveRecord::Base.connection.tables).to include('some')

          quietly { migration_context.up(2) }

          expect(ActiveRecord::Base.connection.tables).not_to include('some')
        end
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

    describe 'change column' do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'change_column')
      migration_version = "#{ActiveRecord::VERSION::MAJOR}.#{ActiveRecord::VERSION::MINOR}"

      before(:all) { FileUtils.mkdir_p migrations_dir }
      after(:all) { FileUtils.rm_rf migrations_dir }

      subject(:migrate_and_query_schema) do
        migrations.each do |name, body|
          File.write(File.join(migrations_dir, name), body)
        end

        quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }

        schema(model)
      end

      describe 'default' do
        let(:migrations) do
          { '1_create_table.rb' => <<~RUBY }
            class CreateTable < ActiveRecord::Migration[#{migration_version}]
              def change
                create_table :some, id: false, options: 'MergeTree ORDER BY date' do |t|
                  t.date :date, null: false
                  t.integer :new_column, default: 1
                end
              end
            end
          RUBY
        end

        it 'sets new default with basic syntax' do
          migrations['2_change_column_default.rb'] = <<~RUBY
            class ChangeColumnDefault < ActiveRecord::Migration[#{migration_version}]
              def change
                change_column_default :some, :new_column, 200
              end
            end
          RUBY

          current_schema = migrate_and_query_schema
          expect(current_schema['new_column'].default).to eq('200')
        end

        it 'sets new default with hash syntax' do
          migrations['2_change_column_default.rb'] = <<~RUBY
            class ChangeColumnDefault < ActiveRecord::Migration[#{migration_version}]
              def change
                change_column_default :some, :new_column, from: 1, to: 200
              end
            end
          RUBY

          current_schema = migrate_and_query_schema
          expect(current_schema['new_column'].default).to eq('200')
        end

        it 'removes default with basic syntax' do
          migrations['2_change_column_default.rb'] = <<~RUBY
            class ChangeColumnDefault < ActiveRecord::Migration[#{migration_version}]
              def change
                change_column_default :some, :new_column, nil
              end
            end
          RUBY

          current_schema = migrate_and_query_schema
          expect(current_schema['new_column'].default).to be_nil
        end

        it 'removes default with hash syntax' do
          migrations['2_change_column_default.rb'] = <<~RUBY
            class ChangeColumnDefault < ActiveRecord::Migration[#{migration_version}]
              def change
                change_column_default :some, :new_column, from: 1, to: nil
              end
            end
          RUBY

          current_schema = migrate_and_query_schema
          expect(current_schema['new_column'].default).to be_nil
        end
      end

      describe 'null' do
        context 'when column is initially nullable' do
          let(:migrations) do
            { '1_create_table.rb' => <<~RUBY }
              class CreateTable < ActiveRecord::Migration[#{migration_version}]
                def change
                  create_table :some, id: false, options: 'MergeTree ORDER BY date' do |t|
                    t.date :date, null: false
                    t.integer :new_column, default: 1, null: true
                  end
                end
              end
            RUBY
          end

          it 'lets you make the column non-nullable' do
            migrations['2_change_column_default.rb'] = <<~RUBY
              class ChangeColumnDefault < ActiveRecord::Migration[#{migration_version}]
                def change
                  change_column_null :some, :new_column, false
                end
              end
            RUBY

            current_schema = migrate_and_query_schema
            expect(current_schema['new_column'].null).to be_falsey
          end

          it 'raises an error when the fourth (default) argument is included' do
            migrations['2_change_column_default.rb'] = <<~RUBY
              class ChangeColumnDefault < ActiveRecord::Migration[#{migration_version}]
                def change
                  change_column_null :some, :new_column, false, 3
                end
              end
            RUBY

            expect { migrate_and_query_schema }.to raise_error(/Cannot set temporary default when changing column nullability/)
          end
        end

        context 'when column is initially non-nullable' do
          let(:migrations) do
            { '1_create_table.rb' => <<~RUBY }
              class CreateTable < ActiveRecord::Migration[#{migration_version}]
                def change
                  create_table :some, id: false, options: 'MergeTree ORDER BY date' do |t|
                    t.date :date, null: false
                    t.integer :new_column, default: 1, null: false
                  end
                end
              end
            RUBY
          end

          it 'lets you make the column nullable' do
            migrations['2_change_column_default.rb'] = <<~RUBY
              class ChangeColumnDefault < ActiveRecord::Migration[#{migration_version}]
                def change
                  change_column_null :some, :new_column, true
                end
              end
            RUBY

            current_schema = migrate_and_query_schema
            expect(current_schema['new_column'].null).to be_truthy
          end

          it 'ignores the fourth (default) argument' do
            migrations['2_change_column_default.rb'] = <<~RUBY
              class ChangeColumnDefault < ActiveRecord::Migration[#{migration_version}]
                def change
                  change_column_null :some, :new_column, true, 3
                end
              end
            RUBY

            current_schema = migrate_and_query_schema
            expect(current_schema['new_column'].null).to be_truthy
            expect(current_schema['new_column'].default).to eq('1')
          end
        end
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

          expect(ActiveRecord::Base.connection.functions).to match_array(['addFun', 'multFun'])
        end
      end

      context 'dsl' do
        let(:directory) { 'dsl_create_function' }

        it 'creates a function' do
          ActiveRecord::Base.connection.execute('CREATE FUNCTION forced_fun AS (x, k, b) -> k*x + b')

          subject

          expect(ActiveRecord::Base.connection.functions).to match_array(['forced_fun', 'some_fun'])
          expect(ActiveRecord::Base.connection.show_create_function('forced_fun').chomp).to eq('CREATE FUNCTION forced_fun AS (x, y) -> (x + y)')
        end
      end
    end
  end
end
