require 'rails/generators/active_record/migration/migration_generator'

class ClickhouseMigrationGenerator < ActiveRecord::Generators::MigrationGenerator
  source_root File.join(File.dirname(ActiveRecord::Generators::MigrationGenerator.instance_method(:create_migration_file).source_location.first), "templates")

  def create_migration_file
    set_local_assigns!
    validate_file_name!
    migration_template @migration_template, File.join(db_migrate_path, "#{file_name}.rb")
  end

  private

  def db_migrate_path
    if defined?(Rails.application) && Rails.application && respond_to?(:configured_migrate_path, true)
      configured_migrate_path || default_migrate_path
    else
      default_migrate_path
    end
  end

  def default_migrate_path
    "db/migrate_clickhouse"
  end
end
