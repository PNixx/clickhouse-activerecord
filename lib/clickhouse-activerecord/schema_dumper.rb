module ClickhouseActiverecord
  class SchemaDumper < ::ActiveRecord::ConnectionAdapters::SchemaDumper

    def table(table, stream)
      stream.puts "  # TABLE: #{table}"
      stream.puts "  # SQL: #{@connection.do_system_execute("SHOW CREATE TABLE `#{table.gsub(/^\.inner\./, '')}`")['data'].try(:first).try(:first)}"
      super(table.gsub(/^\.inner\./, ''), stream)
    end
  end
end
