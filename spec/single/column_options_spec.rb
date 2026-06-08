# frozen_string_literal: true

RSpec.describe 'ColumnOptions' do
  let(:connection) { ActiveRecord::Base.connection }

  describe '#valid_column_definition_options' do
    it 'includes :first and :after' do
      td = ActiveRecord::ConnectionAdapters::Clickhouse::TableDefinition.new(connection, :test)
      options = td.send(:valid_column_definition_options)
      expect(options).to include(:first, :after)
    end

    it 'includes all clickhouse-specific options' do
      td = ActiveRecord::ConnectionAdapters::Clickhouse::TableDefinition.new(connection, :test)
      options = td.send(:valid_column_definition_options)
      expect(options).to include(:array, :low_cardinality, :fixed_string, :value, :type, :map, :codec, :unsigned)
    end
  end

  describe '#create_column_definition' do
    let(:td) { ActiveRecord::ConnectionAdapters::Clickhouse::TableDefinition.new(connection, :test) }

    it 'creates a ColumnDefinition with sql_type and cast_type' do
      cd = td.send(:create_column_definition, :name, :string, null: false)
      expect(cd).to be_a(ActiveRecord::ConnectionAdapters::ColumnDefinition)
      expect(cd.name).to eq(:name)
      expect(cd.type).to eq(:string)
      expect(cd.sql_type).to be_a(String)
      expect(cd.sql_type).to eq('String')
    end

    it 'accepts :first and :after in options' do
      expect {
        td.send(:create_column_definition, :col, :integer, first: true)
      }.not_to raise_error

      expect {
        td.send(:create_column_definition, :col, :integer, after: :other_col)
      }.not_to raise_error
    end

    it 'accepts clickhouse-specific options' do
      expect {
        td.send(:create_column_definition, :col, :string, low_cardinality: true)
      }.not_to raise_error

      expect {
        td.send(:create_column_definition, :col, :string, codec: 'LZ4')
      }.not_to raise_error

      expect {
        td.send(:create_column_definition, :col, :integer, unsigned: false)
      }.not_to raise_error
    end
  end

  describe '#integer with :first and :after' do
    it 'works with migration context' do
      expect {
        connection.create_table(:test_positions, id: false, force: true) do |t|
          t.integer :id, null: false
          t.string :name, null: false
          t.integer :position, first: true
        end
      }.not_to raise_error

      expect(connection.tables).to include('test_positions')
    end

    after do
      connection.drop_table(:test_positions) rescue nil
    end
  end
end
