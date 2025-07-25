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
      functions = @connection.functions.sort
      functions.each do |function|
        function(function, stream)
      end

      view_tables = @connection.views.sort
      materialized_view_tables = @connection.materialized_views.sort
      sorted_tables = @connection.tables.sort - view_tables - materialized_view_tables

      (sorted_tables + view_tables + materialized_view_tables).each do |table_name|
        table(table_name, stream) unless ignored?(table_name)
      end
    end

    def table(table, stream)
      if table.match(/^\.inner/).nil?
        sql= ""
        simple ||= ENV['simple'] == 'true'
        unless simple
          stream.puts "  # TABLE: #{table}"
          sql = @connection.show_create_table(table)
          stream.puts "  # SQL: #{sql.gsub(/ENGINE = Replicated(.*?)\('[^']+',\s*'[^']+',?\s?([^\)]*)?\)/, "ENGINE = \\1(\\2)")}" if sql
          # super(table.gsub(/^\.inner\./, ''), stream)

          # detect view table
          view_match = sql.match(/^CREATE\s+(MATERIALIZED\s+)?VIEW\s+\S+\s+(?:TO (\S+))?/)
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
            tbl.print ', view: true' if view_match
            tbl.print ', materialized: true' if view_match && view_match[1].presence
            tbl.print ", to: \"#{view_match[2]}\"" if view_match && view_match[2].presence
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
          if simple || !view_match
            columns.each do |column|
              raise StandardError, "Unknown type '#{column.sql_type}' for column '#{column.name}'" unless @connection.valid_type?(column.type)
              next if column.name == pk && column.name == "id"
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

    def column_spec_for_primary_key(column)
      spec = super

      id = ActiveRecord::ConnectionAdapters::ClickhouseAdapter::NATIVE_DATABASE_TYPES.invert[{name: column.sql_type.gsub(/\(\d+\)/, "")}]
      spec[:id] = id.inspect if id.present?

      spec.except!(:limit, :unsigned) # This can be removed at some date, it is only here to clean up existing schemas which have dumped these values already
    end

    def function(function, stream)
      stream.puts "  # FUNCTION: #{function}"
      sql = @connection.show_create_function(function)
      if sql
        stream.puts "  # SQL: #{sql}"
        stream.puts "  create_function \"#{function}\", \"#{sql.sub(/\ACREATE(OR REPLACE)? FUNCTION .*? AS/, '').strip}\", force: true"
        stream.puts
      end
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
      (column.sql_type =~ /Array\(/).nil? ? nil : true
    end

    def schema_map(column)
      if column.sql_type =~ /Map\(([^,]+),\s*(Array)\)/
        return :array
      end

      (column.sql_type =~ /Map\(/).nil? ? nil : true
    end

    def schema_low_cardinality(column)
      (column.sql_type =~ /LowCardinality\(/).nil? ? nil : true
    end

    # @param [ActiveRecord::ConnectionAdapters::Clickhouse::Column] column
    def prepare_column_options(column)
      spec = {}
      spec[:unsigned] = schema_unsigned(column)
      spec[:array] = schema_array(column)
      spec[:map] = schema_map(column)
      if spec[:map] == :array
        spec[:array] = nil
      end
      spec[:low_cardinality] = schema_low_cardinality(column)
      spec[:codec] = column.codec.inspect if column.codec
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
