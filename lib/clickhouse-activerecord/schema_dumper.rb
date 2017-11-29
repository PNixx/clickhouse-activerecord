class ClickhouseActiverecord::SchemaDumper < ActiveRecord::SchemaDumper

  def table(table, stream)

    stream.puts "  # TABLE: #{table}"
    stream.puts "  # SQL: #{@connection.query("SHOW CREATE TABLE #{table.gsub(/^\.inner\./, '')}")['data'].try(:first).try(:first)}"
    super(table.gsub(/^\.inner\./, ''), stream)

  end
end
