# frozen_string_literal: true

RSpec.describe 'Cluster Migration', :migrations, cluster: true do
  let(:model) do
    Class.new(ActiveRecord::Base) do
      self.table_name = 'some'
    end
  end
  let(:directory) { raise 'NotImplemented' }
  let(:migrations_dir) { File.join(FIXTURES_PATH, 'migrations', directory) }
  let(:migration_context) { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration, model.connection.internal_metadata) }

  connection_config =
    if ActiveRecord::version >= Gem::Version.new('6.1')
      ActiveRecord::Base.connection_db_config.configuration_hash
    else
      ActiveRecord::Base.connection_config
    end

  before(:all) do
    ActiveRecord::Base.establish_connection(connection_config.merge(cluster_name: '{cluster}'))
  end

  after(:all) do
    ActiveRecord::Base.establish_connection(connection_config)
  end

  describe 'performs migrations' do
    before(:all) do
      raise 'Unknown cluster name in config' if connection_config[:cluster_name].blank?
    end

    subject do
      quietly { migration_context.up }
    end

    context 'dsl' do
      context 'with distributed' do
        let(:model_distributed) do
          Class.new(ActiveRecord::Base) do
            self.table_name = 'some_distributed'
          end
        end
        let(:directory) { 'dsl_create_table_with_distributed' }

        it 'creates a table with distributed table' do
          subject

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
          subject

          expect(ActiveRecord::Base.connection.tables).to include('some')
          expect(ActiveRecord::Base.connection.tables).to include('some_distributed')

          quietly do
            migration_context.down
          end

          expect(ActiveRecord::Base.connection.tables).not_to include('some')
          expect(ActiveRecord::Base.connection.tables).not_to include('some_distributed')
        end
      end
    end

    context 'with alias in cluster_name' do
      let(:directory) { 'dsl_create_table_with_cluster_name_alias' }

      it 'creates a table' do
        subject

        current_schema = schema(model)

        expect(current_schema.keys.count).to eq(1)
        expect(current_schema).to have_key('date')
        expect(current_schema['date'].sql_type).to eq('Date')
      end

      it 'drops a table' do
        subject

        expect(ActiveRecord::Base.connection.tables).to include('some')

        quietly do
          migration_context.down
        end

        expect(ActiveRecord::Base.connection.tables).not_to include('some')
      end
    end
  end
end
