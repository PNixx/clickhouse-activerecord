# frozen_string_literal: true

module ClickhouseActiverecord
  # Builds the ordered list of `CREATE` statements that makes up `structure.sql`.
  #
  # Rails 8 prepares an empty database by loading `structure.sql` instead of
  # replaying every migration, so the file has to be replayable top to bottom.
  # ClickHouse resolves a view's query when the view is created and refuses to
  # create a materialized view whose `TO` target is missing, which a dump grouped
  # by kind and sorted by name only satisfies by luck. Statements are therefore
  # ordered by a dependency graph built from `system.tables` metadata and from
  # the `CREATE` statements themselves.
  class StructureDumper
    Definition = Struct.new(:name, :kind, :sql, :dependencies, keyword_init: true)

    KIND_RANK = %i[function table dictionary view materialized_view].freeze

    KIND_PATTERNS = {
      materialized_view: /\ACREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b/i,
      view: /\ACREATE\s+(?:OR\s+REPLACE\s+)?(?:LIVE\s+|WINDOW\s+)?VIEW\b/i,
      dictionary: /\ACREATE\s+(?:OR\s+REPLACE\s+)?DICTIONARY\b/i
    }.freeze

    NAME = /(?:`[^`]+`|[A-Za-z_]\w*)/
    QUALIFIED_NAME = /(?:(#{NAME})\.)?(#{NAME})/

    # Table functions (`FROM numbers(10)`) and `ARRAY JOIN`, whose operand is a
    # column rather than a table, are left out.
    FROM_OR_JOIN = /\b(?:FROM|(?<!ARRAY\s)JOIN)\s+#{QUALIFIED_NAME}(?!\s*\()/i
    MATERIALIZED_TARGET = /\ACREATE\s+MATERIALIZED\s+VIEW\s+#{QUALIFIED_NAME}(?:\s+UUID\s+'[^']*')?\s+TO\s+#{QUALIFIED_NAME}/i
    BUFFER_ENGINE = /\bENGINE\s*=\s*Buffer\s*\(\s*'?(#{NAME})'?\s*,\s*'?(#{NAME})'?/i
    DICTIONARY_ENGINE = /\bENGINE\s*=\s*Dictionary\s*\(\s*'?#{QUALIFIED_NAME}'?\s*\)/i

    def initialize(connection, database)
      @connection = connection
      @database = database
    end

    def dump(path)
      File.open(path, 'w:utf-8') do |file|
        statements.each { |sql| file.puts "#{sql};\n\n" }
      end
    end

    def statements
      (order(function_definitions) + order(table_definitions)).map(&:sql)
    end

    private

    def function_definitions
      rows = @connection.execute(<<~SQL)['data']
        SELECT name, create_query FROM system.functions WHERE origin = 'SQLUserDefined' ORDER BY name
      SQL

      definitions = rows.map do |name, create_query|
        Definition.new(name: name, kind: :function, sql: create_query.gsub('\\n', "\n"), dependencies: Set.new)
      end

      names = definitions.map(&:name)
      definitions.each do |definition|
        calls = names.select { |name| name != definition.name && definition.sql.match?(/\b#{Regexp.escape(name)}\s*\(/) }
        definition.dependencies = Set.new(calls)
      end

      definitions
    end

    def table_definitions
      rows = table_rows
      names = Set.new(rows.map(&:first))

      definitions = rows.map do |name, *|
        sql = @connection.show_create_table(name, single_line: false).gsub("#{@database}.", '')
        Definition.new(name: name, kind: kind_of(sql), sql: sql, dependencies: Set.new)
      end
      by_name = definitions.index_by(&:name)

      definitions.each do |definition|
        references(definition.sql).each do |reference|
          definition.dependencies << reference if names.include?(reference) && reference != definition.name
        end
      end

      rows.each do |name, loading_databases, loading_tables, dependent_databases, dependent_tables|
        loading_databases.zip(loading_tables).each do |database, table|
          next unless database == @database && names.include?(table) && table != name

          by_name[name].dependencies << table
        end

        # `dependencies_*` points the other way: those objects select from `name`.
        dependent_databases.zip(dependent_tables).each do |database, table|
          next unless database == @database && names.include?(table) && table != name

          by_name[table].dependencies << name
        end
      end

      definitions
    end

    def table_rows
      @connection.execute(<<~SQL)['data']
        SELECT
          name,
          loading_dependencies_database,
          loading_dependencies_table,
          dependencies_database,
          dependencies_table
        FROM system.tables
        WHERE database = #{@connection.quote(@database)} AND name NOT LIKE '.inner_id.%'
        ORDER BY name
      SQL
    rescue ActiveRecord::StatementInvalid
      # `loading_dependencies_*` were added in ClickHouse 22.4.
      @connection.execute(<<~SQL)['data'].map { |name, *dependencies| [name, [], [], *dependencies] }
        SELECT name, dependencies_database, dependencies_table
        FROM system.tables
        WHERE database = #{@connection.quote(@database)} AND name NOT LIKE '.inner_id.%'
        ORDER BY name
      SQL
    end

    def order(definitions)
      pending = definitions.sort_by { |definition| [KIND_RANK.index(definition.kind), definition.name] }
      emitted = Set.new
      ordered = []

      until pending.empty?
        ready, pending = pending.partition { |definition| definition.dependencies.subset?(emitted) }

        # Nothing is ready only when the definitions left depend on each other. A
        # cyclic schema has no replayable order, so emit the first one and move
        # on rather than loop forever.
        ready, pending = [pending.first], pending.drop(1) if ready.empty?

        ordered.concat(ready)
        emitted.merge(ready.map(&:name))
      end

      ordered
    end

    def kind_of(sql)
      KIND_PATTERNS.find { |_kind, pattern| sql.match?(pattern) }&.first || :table
    end

    def references(sql)
      [
        scan(sql, FROM_OR_JOIN).map { |match| qualified_name(match, 1) },
        scan(sql, MATERIALIZED_TARGET).map { |match| qualified_name(match, 3) },
        scan(sql, BUFFER_ENGINE).map { |match| qualified_name(match, 1) },
        scan(sql, DICTIONARY_ENGINE).map { |match| qualified_name(match, 1) }
      ].flatten.compact
    end

    def scan(sql, regexp)
      sql.to_enum(:scan, regexp).map { Regexp.last_match }
    end

    def qualified_name(match, index)
      database = match[index]
      return if database && unquote(database) != @database

      unquote(match[index + 1])
    end

    def unquote(name)
      name.start_with?('`') ? name[1..-2] : name
    end
  end
end
