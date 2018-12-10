class ClickhouseTasks
  class << self
    delegate :connection, :establish_connection, to: ActiveRecord::Base

    def create(env = Rails.env)
      establish_connection configuration_without_database(env)

      begin
        db_name = configuration(env)['database']
        q = format('CREATE DATABASE "%s"', db_name)
        connection.execute(q)

        puts format('Database "%s" created', db_name)
      rescue ActiveRecord::ActiveRecordError => e
        puts e.message
      end
    end

    def drop(env = Rails.env)
      establish_connection configuration_without_database(env)

      begin
        db_name = configuration(env)['database']
        q = format('DROP DATABASE IF EXISTS "%s"', db_name)
        connection.execute(q)

        puts format('Database "%s" droped', db_name)
      rescue ActiveRecord::ActiveRecordError => e
        puts e.message
      end
    end

    def purge(env = Rails.env)
      with_captured_stdout do
        drop(env)
        create(env)
      end
      puts format('Database "%s" purged', configuration(env)['database'])
    end

    def structure_load(env = Rails.env)
      with_captured_stdout do
        purge(env)
      end
      establish_connection configuration(env)
      
      File.read(Rails.root.join('db', 'clickhouse_structure.sql')).split(';').each do |q|
        q = q.strip
        connection.execute(q) if q.length > 0
      end

      puts format('Database "%s" structure loaded', configuration(env)['database'])
    end

    def structure_dump(env = Rails.env)
      establish_connection configuration(env)

      File.open(Rails.root.join('db', 'clickhouse_structure.sql'), 'w:utf-8') do |file|
        connection.execute('SHOW TABLES')['data'].each do |tbl|
          tbl = tbl[0]
          file << format("DROP TABLE IF EXISTS \"%s\";\n", tbl)
          crt = connection.execute(format('SHOW CREATE TABLE "%s"', tbl))['data'][0][0]
          crt = crt.gsub(/^(CREATE TABLE )\w+\./, "\\1")
          file << format("%s;\n\n", crt)
        end
      end

      puts format('Database "%s" structure dumped', configuration(env)['database'])
    end

    def schema_dump(env = Rails.env)
      establish_connection configuration(env)

      File.open(Rails.root.join('db', 'clickhouse_schema.rb'), 'w:utf-8') do |file|
        ActiveRecord::SchemaDumper.dump(connection, file)
      end

      puts format('Database "%s" schema dumped', configuration(env)['database'])
    end

    private

    def configuration(env = Rails.env)
      Rails.configuration.database_configuration[format('%s_clickhouse', env)]
    end

    def configuration_without_database(env = Rails.env)
      configuration(env).merge("database" => nil)
    end

    def with_captured_stdout
      original_stdout = $stdout
      $stdout = StringIO.new
      yield
      $stdout.string
    ensure
      $stdout = original_stdout
    end
  end
end