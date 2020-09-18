module ClickhouseActiverecord
  class SchemaDumper < ::ActiveRecord::ConnectionAdapters::SchemaDumper

    def header(stream)
      stream.puts <<HEADER
# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `rails
# clickhouse:schema:load`. When creating a new database, `rails clickhouse:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ClickhouseActiverecord::Schema.define(#{define_params}) do

HEADER
    end

    def table(table, stream)
      if table.match(/^\.inner\./).nil?
        stream.puts "  # TABLE: #{table}"
        sql = @connection.do_system_execute("SHOW CREATE TABLE `#{table.gsub(/^\.inner\./, '')}`")['data'].try(:first).try(:first)
        stream.puts "  # SQL: #{sql.gsub(/ENGINE = Replicated(.*?)\('[^']+',\s*'[^']+',?\s?([^\)]*)?\)/, "ENGINE = \\1(\\2)")}" if sql
        # super(table.gsub(/^\.inner\./, ''), stream)

        # detect view table
        match = sql.match(/^CREATE\s+(MATERIALIZED)\s+VIEW/)

        # Copy from original dumper
        columns = @connection.columns(table)
        begin
          tbl = StringIO.new

          # first dump primary key column
          pk = @connection.primary_key(table)

          tbl.print "  create_table #{remove_prefix_and_suffix(table).inspect}"

          # Add materialize flag
          tbl.print ', view: true' if match
          tbl.print ', materialized: true' if match && match[1].presence

          case pk
          when String
            tbl.print ", primary_key: #{pk.inspect}" unless pk == "id"
            pkcol = columns.detect { |c| c.name == pk }
            pkcolspec = column_spec_for_primary_key(pkcol)
            if pkcolspec.present?
              tbl.print ", #{format_colspec(pkcolspec)}"
            end
          when Array
            tbl.print ", primary_key: #{pk.inspect}"
          else
            tbl.print ", id: false"
          end

          table_options = @connection.table_options(table)
          if table_options.present?
            tbl.print ", #{format_options(table_options)}"
          end

          tbl.puts ", force: :cascade do |t|"

          # then dump all non-primary key columns
          columns.each do |column|
            raise StandardError, "Unknown type '#{column.sql_type}' for column '#{column.name}'" unless @connection.valid_type?(column.type)
            next if column.name == pk
            type, colspec = column_spec(column)
            tbl.print "    t.#{type} #{column.name.inspect}"
            tbl.print ", #{format_colspec(colspec)}" if colspec.present?
            tbl.puts
          end

          indexes_in_create(table, tbl)

          tbl.puts "  end"
          tbl.puts

          tbl.rewind
          stream.print tbl.read
        rescue => e
          stream.puts "# Could not dump table #{table.inspect} because of following #{e.class}"
          stream.puts "#   #{e.message}"
          stream.puts
        end
      end
    end

    def format_options(options)
      if options && options[:options]
        options[:options] = options[:options].gsub(/^Replicated(.*?)\('[^']+',\s*'[^']+',?\s?([^\)]*)?\)/, "\\1(\\2)")
      end
      super
    end
  end
end
