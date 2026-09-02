# frozen_string_literal: true

require 'clickhouse-activerecord/structure_dumper'

module ClickhouseActiverecord
  class Tasks
    delegate :connection, :establish_connection, to: ActiveRecord::Base

    def self.using_database_configurations?
      true
    end

    def initialize(configuration)
      @configuration = configuration
    end

    def create
      establish_master_connection
      connection.create_database @configuration.database
    rescue ActiveRecord::StatementInvalid => e
      if e.cause.to_s.include?('already exists')
        raise ActiveRecord::DatabaseAlreadyExists
      else
        raise
      end
    end

    def drop
      establish_master_connection
      connection.drop_database @configuration.database
    end

    def purge
      ActiveRecord::Base.connection_handler.clear_active_connections!(:all)
      drop
      create
    end

    def structure_dump(path, *)
      establish_master_connection

      StructureDumper.new(connection, @configuration.database).dump(path)
    end

    def structure_load(*args)
      File.read(args.first).split(";\n\n").each do |sql|
        if sql.gsub(/[a-z]/i, '').blank?
          next
        elsif sql =~ /^INSERT INTO/
          connection.execute(sql, nil, format: nil)
        elsif sql =~ /^CREATE .*?FUNCTION/
          connection.execute(sql, nil, format: nil)
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

    def check_current_protected_environment!(db_config, migration_class = ActiveRecord::Migration)
      with_temporary_pool(db_config, migration_class) do |pool|
        migration_context = pool.migration_context
        current = migration_context.current_environment
        stored  = migration_context.last_stored_environment

        if migration_context.protected_environment?
          raise ActiveRecord::ProtectedEnvironmentError.new(stored)
        end

        if stored && stored != current
          raise ActiveRecord::EnvironmentMismatchError.new(current: current, stored: stored)
        end
      rescue ActiveRecord::NoDatabaseError
      end
    end

    private

    def with_temporary_pool(db_config, migration_class, clobber: false)
      original_db_config = migration_class.connection_db_config
      pool = migration_class.connection_handler.establish_connection(db_config, clobber: clobber)

      yield pool
    ensure
      migration_class.connection_handler.establish_connection(original_db_config, clobber: clobber)
    end

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
