module ClickhouseActiverecord
  class SchemaDumper < ::ActiveRecord::ConnectionAdapters::SchemaDumper

    attr_accessor :simple

    class << self
      def dump(connection = ActiveRecord::Base.connection, stream = STDOUT, config = ActiveRecord::Base, default = false)
        dumper = connection.create_schema_dumper(generate_options(config))
        dumper.simple = default
        dumper.dump(stream)
        stream
      end
    end

    private

    def tables(stream)
      functions = @connection.functions
      functions.each do |function|
        function(function, stream)
      end

      sorted_tables = @connection.tables.sort {|a,b| @connection.show_create_table(a).match(/^CREATE\s+(MATERIALIZED\s+)?VIEW/) ? 1 : a <=> b }
      sorted_tables.each do |table_name|
        table(table_name, stream) unless ignored?(table_name)
      end
    end

    def table(table, stream)
      if table.match(/^\.inner/).nil?
        unless simple
          stream.puts "  # TABLE: #{table}"
          sql = @connection.show_create_table(table)
          stream.puts "  # SQL: #{sql.gsub(/ENGINE = Replicated(.*?)\('[^']+',\s*'[^']+',?\s?([^\)]*)?\)/, "ENGINE = \\1(\\2)")}" if sql
          # super(table.gsub(/^\.inner\./, ''), stream)

          # detect view table
          match = sql.match(/^CREATE\s+(MATERIALIZED\s+)?VIEW/)
        end

        # Copy from original dumper
        columns = @connection.columns(table)
        begin
          tbl = StringIO.new

          # first dump primary key column
          pk = @connection.primary_key(table)

          tbl.print "  create_table #{remove_prefix_and_suffix(table).inspect}"

          unless simple
            # Add materialize flag
            tbl.print ', view: true' if match
            tbl.print ', materialized: true' if match && match[1].presence
          end

          if (id = columns.detect { |c| c.name == 'id' })
            spec = column_spec_for_primary_key(id)
            if spec.present?
              tbl.print ", #{format_colspec(spec)}"
            end
          else
            tbl.print ", id: false"
          end

          unless simple
            table_options = @connection.table_options(table)
            if table_options.present?
              table_options = format_options(table_options)
              table_options.gsub!(/Buffer\('[^']+'/, 'Buffer(\'#{connection.database}\'')
              tbl.print ", #{table_options}"
            end
          end

          tbl.puts ", force: :cascade do |t|"

          # then dump all non-primary key columns
          if simple || !match
            columns.each do |column|
              raise StandardError, "Unknown type '#{column.sql_type}' for column '#{column.name}'" unless @connection.valid_type?(column.type)
              next if column.name == pk
              type, colspec = column_spec(column)
              name = column.name =~ (/\./) ? "\"`#{column.name}`\"" : column.name.inspect
              tbl.print "    t.#{type} #{name}"
              tbl.print ", #{format_colspec(colspec)}" if colspec.present?
              tbl.puts
            end
          end

          indexes = sql.scan(/INDEX \S+ \S+ TYPE .*? GRANULARITY \d+/)
          if indexes.any?
            tbl.puts ''
            indexes.flatten.map!(&:strip).each do |index|
              tbl.puts "    t.index #{index_parts(index).join(', ')}"
            end
          end

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

    def function(function, stream)
      stream.puts "  # FUNCTION: #{function}"
      sql = @connection.show_create_function(function)
      stream.puts "  # SQL: #{sql}" if sql
      stream.puts "  create_function \"#{function}\", \"#{sql.gsub(/^CREATE FUNCTION (.*?) AS/, '').strip}\", force: true" if sql
    end

    def format_options(options)
      if options && options[:options]
        options[:options].gsub!(/^Replicated(.*?)\('[^']+',\s*'[^']+',?\s?([^\)]*)?\)/, "\\1(\\2)")
      end
      super
    end

    def format_colspec(colspec)
      if simple
        super.gsub(/CAST\('?([^,']*)'?,\s?'.*?'\)/, "\\1")
      else
        super
      end
    end

    def schema_limit(column)
      return nil if column.type == :float
      super
    end

    def schema_unsigned(column)
      return nil unless column.type == :integer && !simple
      (column.sql_type =~ /(Nullable)?\(?UInt\d+\)?/).nil? ? false : nil
    end

    def schema_array(column)
      (column.sql_type =~ /Array?\(/).nil? ? nil : true
    end

    def schema_map(column)
      (column.sql_type =~ /Map?\(/).nil? ? nil : true
    end

    def schema_low_cardinality(column)
      (column.sql_type =~ /LowCardinality?\(/).nil? ? nil : true
    end

    def prepare_column_options(column)
      spec = {}
      spec[:unsigned] = schema_unsigned(column)
      spec[:array] = schema_array(column)
      spec[:map] = schema_map(column)
      spec[:low_cardinality] = schema_low_cardinality(column)
      spec.merge(super).compact
    end

    def index_parts(index)
      idx = index.match(/^INDEX (?<name>\S+) (?<expr>.*?) TYPE (?<type>.*?) GRANULARITY (?<granularity>\d+)$/)
      index_parts = [
        format_index_parts(idx['expr']),
        "name: #{format_index_parts(idx['name'])}",
        "type: #{format_index_parts(idx['type'])}",
      ]
      index_parts << "granularity: #{idx['granularity']}" if idx['granularity']
      index_parts
    end
  end
end
