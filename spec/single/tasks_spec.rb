# frozen_string_literal: true

require 'clickhouse-activerecord/tasks'

RSpec.describe ClickhouseActiverecord::Tasks do
  let(:configuration) do
    instance_double(
      ActiveRecord::DatabaseConfigurations::HashConfig,
      database: database,
      configuration_hash: configuration_hash,
    )
  end
  let(:tasks) { described_class.new(configuration) }

  describe '#database_name' do
    subject { tasks.send(:database_name) }

    context 'when database is set explicitly in config' do
      let(:database) { 'my_database' }
      let(:configuration_hash) { { url: nil } }

      it { is_expected.to eq('my_database') }
    end

    context 'when database is nil but present in url' do
      let(:database) { nil }
      let(:configuration_hash) { { url: 'clickhouse://user:pass@host:8123/my_database' } }

      it { is_expected.to eq('my_database') }
    end

    context 'when database is empty string but present in url' do
      let(:database) { '' }
      let(:configuration_hash) { { url: 'clickhouse://user:pass@host:8123/my_database' } }

      it { is_expected.to eq('my_database') }
    end

    context 'when both database and url are set, explicit database takes precedence' do
      let(:database) { 'explicit_database' }
      let(:configuration_hash) { { url: 'clickhouse://user:pass@host:8123/url_database' } }

      it { is_expected.to eq('explicit_database') }
    end
  end
end
