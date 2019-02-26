# frozen_string_literal: true

module ClickhouseActiverecord
  class Tasks

    delegate :connection, :establish_connection, :clear_active_connections!, to: ActiveRecord::Base

    def initialize(configuration)
      @configuration = configuration
    end

    def create
      establish_master_connection
      connection.create_database @configuration["database"]
    rescue ActiveRecord::StatementInvalid => e
      if e.cause.to_s.include?('already exists')
        raise ActiveRecord::Tasks::DatabaseAlreadyExists
      else
        raise
      end
    end

    def drop
      establish_master_connection
      connection.drop_database @configuration["database"]
    end

    def purge
      clear_active_connections!
      drop
      create
    end

    def migrate
      check_target_version

      verbose = ENV["VERBOSE"] ? ENV["VERBOSE"] != "false" : true
      scope = ENV["SCOPE"]
      verbose_was, ActiveRecord::Migration.verbose = ActiveRecord::Migration.verbose, verbose
      binding.pry
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
