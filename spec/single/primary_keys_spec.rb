require 'spec_helper'

RSpec.describe 'Primary keys', :migrations do
  let(:connection) { ActiveRecord::Base.connection }

  before do
    connection.execute('CREATE TABLE pk_test (id UInt64, name String) ENGINE = MergeTree ORDER BY id')
  end

  describe '#primary_keys' do
    {
      '22.8.5.29' => false,
      '23.3.1.2' => false,
      '23.4.1.1' => true,
      '23.9.1.1' => true,
      '23.10.1.1' => true,
      '23.12.2.59' => true,
      '24.9.2.42' => true,
    }.each do |version, uses_show_columns|
      context "with server version #{version}" do
        before do
          allow(connection).to receive(:server_version).and_return(version)
          allow(connection).to receive(:do_system_execute).and_call_original
          allow(connection).to receive(:table_structure).and_call_original
        end

        it "#{uses_show_columns ? 'reads' : 'does not read'} the key columns from SHOW COLUMNS" do
          expect(connection.primary_keys('pk_test')).to eq(['id'])

          if uses_show_columns
            expect(connection).to have_received(:do_system_execute).with(/SHOW COLUMNS FROM `pk_test`/)
            expect(connection).not_to have_received(:table_structure)
          else
            expect(connection).not_to have_received(:do_system_execute).with(/SHOW COLUMNS/)
            expect(connection).to have_received(:table_structure).with('pk_test')
          end
        end
      end
    end
  end
end
