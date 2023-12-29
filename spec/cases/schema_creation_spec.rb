# frozen_string_literal: true

RSpec.describe 'Schema creation', :migrations do
  let(:model) do
    Class.new(ActiveRecord::Base) do
      self.table_name = 'sample'
    end
  end

  before do
    migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
    quietly { ActiveRecord::MigrationContext.new(migrations_dir, model.connection.schema_migration).up }
  end

  describe 'add column' do
    it 'adds the specified column at the end of the table' do
      model.connection.add_column :sample, :location, :string
      expect(model.columns.map(&:name)).to eq(%w[id event_name event_value enabled date datetime datetime64 location])
    end

    context 'with :after option' do
      it 'adds the specified column in the correct order' do
        model.connection.add_column :sample, :location, :string, after: 'event_name'
        expect(model.columns.map(&:name)).to eq(%w[id event_name location event_value enabled date datetime datetime64])
      end
    end

    context 'with :value option' do
      it 'allows specification of custom values not implemented out-of-the-box' do
        model.connection.add_column :sample, :multidim_array, 'Array', value: 'Array(UInt8)', null: false
        expect(model.columns.last.sql_type).to eq('Array(Array(UInt8))')
      end
    end

    context 'with :fixed_string option' do
      it 'specifies the fixed length of string columns' do
        model.connection.add_column :sample, :sha256_password, :string, fixed_string: 32, null: false
        expect(model.columns.last.sql_type).to eq('FixedString(32)')
      end
    end

    context 'when :null option is true' do
      it 'makes the column nullable' do
        model.connection.add_column :sample, :location, :string, null: true
        expect(model.columns.last.sql_type).to eq('Nullable(String)')
      end
    end

    context 'when :null option is omitted' do
      it 'assumes the column should be nullable' do
        model.connection.add_column :sample, :location, :string
        expect(model.columns.last.sql_type).to eq('Nullable(String)')
      end
    end

    context 'when :null option is false' do
      it 'makes to column not nullable' do
        model.connection.add_column :sample, :location, :string, null: false
        expect(model.columns.last.sql_type).to eq('String')
      end
    end

    context 'with :low_cardinality option' do
      it 'indicates the column is low-cardinality' do
        model.connection.add_column :sample, :planner_name, :string, low_cardinality: true, null: false
        expect(model.columns.last.sql_type).to eq('LowCardinality(String)')
      end
    end

    context 'with :array option' do
      it 'wraps the type in a single-dimensional array' do
        model.connection.add_column :sample, :planner_name, :string, array: true, null: false
        expect(model.columns.last.sql_type).to eq('Array(String)')
      end
    end

    it 'strips any length/size params following the String type' do
      model.connection.add_column :sample, :location, 'String(255)', null: false
      expect(model.columns.last.sql_type).to eq('String')
    end

    context 'when :default option is given' do
      it 'sets a String default value for the column' do
        model.connection.add_column :sample, :location, :string, default: "Bob's House \\ Home", null: false
        expect(model.columns.last.default).to eq("Bob's House \\ Home")
      end

      it 'sets an Int default value for the column' do
        model.connection.add_column :sample, :attendees, :integer, default: 1, null: false
        expect(model.columns.last.default).to eq('1')
      end

      it 'sets a negative Int default value for the column' do
        model.connection.add_column :sample, :cost, :int32, default: -10, null: false
        expect(model.columns.last.default).to eq('-10')
      end

      it 'sets a Float default value for the column' do
        model.connection.add_column :sample, :ticket_price, :float, default: 50.0, null: false
        expect(model.columns.last.default).to eq('50.')
      end

      it 'sets a negative Float default value for the column' do
        model.connection.add_column :sample, :overhead, :float, default: -100.0, null: false
        expect(model.columns.last.default).to eq('-100.')
      end

      it 'sets a "true" boolean literal for the default value of the column' do
        model.connection.add_column :sample, :cool, :boolean, default: true, null: false
        expect(model.columns.last.default).to eq('true')
      end

      it 'sets a "false" boolean literal for the default value of the column' do
        model.connection.add_column :sample, :vip_list, :boolean, default: false, null: false
        expect(model.columns.last.default).to eq('false')
      end
    end
  end
end
