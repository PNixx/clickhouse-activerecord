require 'spec_helper'

RSpec.describe 'Schema Loading', :migrations do
  let(:model) { ActiveRecord::Base }
  let(:connection) { model.connection }
  let(:database) { connection.instance_variable_get(:@config)[:database] }

  describe 'assign_database_to_subquery' do
    after do
      connection.execute('DROP VIEW IF EXISTS test_view')
      connection.execute('DROP TABLE IF EXISTS test_target')
      connection.execute('DROP TABLE IF EXISTS test_source')
    end

    context 'when column name contains "from"' do
      it 'does not mistake column name for FROM keyword' do
        # Bug: The regex /(?<=from)/ matches "from" anywhere in the query,
        # including in column names like "sourced_from". This causes the next
        # identifier (often a function name) to be incorrectly prefixed with
        # the database name.
        #
        # Example bug:
        #   SELECT sourced_from, now() FROM table
        # Would incorrectly become:
        #   SELECT sourced_from, default.now() FROM default.table
        # Causing: Function with name 'default.now' does not exist

        connection.execute(<<~SQL)
          CREATE TABLE test_source (
            id UInt64,
            sourced_from String
          ) ENGINE = MergeTree ORDER BY id
        SQL

        connection.execute(<<~SQL)
          CREATE TABLE test_target (
            id UInt64,
            sourced_from String,
            created_at DateTime
          ) ENGINE = MergeTree ORDER BY id
        SQL

        # This query should work: column ending in "from" followed by a function
        expect {
          connection.create_table :test_view, view: true, materialized: true, to: 'test_target',
                                 as: 'SELECT id, sourced_from, now() AS created_at FROM test_source' do |t|
          end
        }.not_to raise_error
      end

      it 'correctly adds database prefix only to table name' do
        connection.execute(<<~SQL)
          CREATE TABLE test_source (
            id UInt64
          ) ENGINE = MergeTree ORDER BY id
        SQL

        connection.execute(<<~SQL)
          CREATE TABLE test_target (
            id UInt64,
            created_at DateTime
          ) ENGINE = MergeTree ORDER BY id
        SQL

        connection.create_table :test_view, view: true, materialized: true, to: 'test_target',
                               as: 'SELECT id, now() AS created_at FROM test_source' do |t|
        end

        # Verify the view was created successfully
        # Note: show_create_table strips database prefixes, so we just verify
        # that the view exists and the function wasn't broken by incorrect prefixing
        show_create = connection.show_create_table('test_view')

        # Should NOT have database prefix on function name
        expect(show_create).not_to include("#{database}.now")
        expect(show_create).to include('now()')
      end
    end
  end
end
