# frozen_string_literal: true

module ClickhouseActiverecord
  class Tasks
    delegate :connection, :establish_connection, to: ActiveRecord::Base

    def initialize(configuration)
      @configuration = configuration.with_indifferent_access
    end

    def create
      establish_master_connection
      connection.create_database @configuration['database']
    rescue ActiveRecord::StatementInvalid => e
      if e.cause.to_s.include?('already exists')
        raise ActiveRecord::DatabaseAlreadyExists
      else
        raise
      end
    end

    def drop
      establish_master_connection
      connection.drop_database @configuration['database']
    end

    def purge
      ActiveRecord::Base.connection_handler.clear_active_connections!(:all)
      drop
      create
    end

    def structure_dump(*args)
      tables = connection.execute("SHOW TABLES FROM #{@configuration['database']}")['data'].flatten

      File.open(args.first, 'w:utf-8') do |file|
        tables.each do |table|
          next if table.match(/\.inner/)
          file.puts connection.execute("SHOW CREATE TABLE #{table}")['data'].try(:first).try(:first).gsub("#{@configuration['database']}.", '') + ";\n\n"
        end
      end
    end

    def structure_load(*args)
      File.read(args.first).split(";\n\n").each do |sql|
        if sql.gsub(/[a-z]/i, '').blank?
          next
        elsif sql =~ /^INSERT INTO/
          connection.do_execute(sql, nil, format: nil)
        else
          connection.execute(sql)
        end
      end
    end

    def migrate
      check_target_version

      verbose = ENV["VERBOSE"] ? ENV["VERBOSE"] != "false" : true
      scope = ENV["SCOPE"]
      verbose_was, ActiveRecord::Migration.verbose = ActiveRecord::Migration.verbose, verbose
      connection.migration_context.migrate(target_version) do |migration|
        scope.blank? || scope == migration.scope
      end
      ActiveRecord::Base.clear_cache!
    ensure
      ActiveRecord::Migration.verbose = verbose_was
    end

    private

    def establish_master_connection
      establish_connection @configuration
    end

    def check_target_version
      if target_version && !(ActiveRecord::Migration::MigrationFilenameRegexp.match?(ENV["VERSION"]) || /\A\d+\z/.match?(ENV["VERSION"]))
        raise "Invalid format of target version: `VERSION=#{ENV['VERSION']}`"
      end
    end

    def target_version
      ENV["VERSION"].to_i if ENV["VERSION"] && !ENV["VERSION"].empty?
    end
  end
end
