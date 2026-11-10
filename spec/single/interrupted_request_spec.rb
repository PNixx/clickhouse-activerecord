# frozen_string_literal: true

RSpec.describe 'Interrupted request' do
  let(:connection) { ActiveRecord::Base.connection }
  let(:http) { connection.instance_variable_get(:@connection) }
  let(:interrupt) { Class.new(Exception) }

  before do
    connection.select_value('SELECT 1')
    interrupted = false
    allow(Net::HTTPResponse).to receive(:read_new).and_wrap_original do |read_new, socket|
      next read_new.call(socket) if interrupted

      interrupted = true
      raise interrupt
    end
  end

  context 'when an Exception escapes a query before its reply is read' do
    before do
      connection.select_value('SELECT 1')
    rescue interrupt
      nil
    end

    it 'finishes the HTTP session' do
      expect(http.started?).to be(false)
    end

    it 'answers the next query with its own result' do
      expect(connection.select_value('SELECT 42')).to eq(42)
    end

    it 'reopens the keep-alive session for later queries' do
      connection.select_value('SELECT 1')
      expect(http.started?).to be(true)
    end
  end

  context 'when an Exception escapes a streamed query before its reply is read' do
    before do
      connection.execute_to_file('SELECT 1')
    rescue interrupt
      nil
    end

    it 'finishes the HTTP session' do
      expect(http.started?).to be(false)
    end

    it 'answers the next query with its own result' do
      expect(connection.select_value('SELECT 42')).to eq(42)
    end
  end
end
