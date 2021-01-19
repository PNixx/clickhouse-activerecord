# -*- encoding: utf-8 -*-
# stub: clickhouse-activerecord 0.4.9 ruby lib

Gem::Specification.new do |s|
  s.name = "clickhouse-activerecord".freeze
  s.version = "0.4.10"

  s.required_rubygems_version = Gem::Requirement.new(">= 0".freeze) if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib".freeze]
  s.authors = ["Sergey Odintsov".freeze]
  s.bindir = "exe".freeze
  s.date = "2021-01-19"
  s.description = "ActiveRecord adapter for ClickHouse".freeze
  s.email = ["nixx.dj@gmail.com".freeze]
  s.files = [".gitignore".freeze, ".rspec".freeze, "CHANGELOG.md".freeze, "CODE_OF_CONDUCT.md".freeze, "Gemfile".freeze, "LICENSE.txt".freeze, "README.md".freeze, "Rakefile".freeze, "bin/console".freeze, "bin/setup".freeze, "clickhouse-activerecord.gemspec".freeze, "lib/active_record/connection_adapters/clickhouse/oid/big_integer.rb".freeze, "lib/active_record/connection_adapters/clickhouse/oid/date.rb".freeze, "lib/active_record/connection_adapters/clickhouse/oid/date_time.rb".freeze, "lib/active_record/connection_adapters/clickhouse/schema_creation.rb".freeze, "lib/active_record/connection_adapters/clickhouse/schema_definitions.rb".freeze, "lib/active_record/connection_adapters/clickhouse/schema_statements.rb".freeze, "lib/active_record/connection_adapters/clickhouse_adapter.rb".freeze, "lib/clickhouse-activerecord.rb".freeze, "lib/clickhouse-activerecord/arel/table.rb".freeze, "lib/clickhouse-activerecord/arel/visitors/to_sql.rb".freeze, "lib/clickhouse-activerecord/migration.rb".freeze, "lib/clickhouse-activerecord/railtie.rb".freeze, "lib/clickhouse-activerecord/schema.rb".freeze, "lib/clickhouse-activerecord/schema_dumper.rb".freeze, "lib/clickhouse-activerecord/tasks.rb".freeze, "lib/clickhouse-activerecord/version.rb".freeze, "lib/generators/clickhouse_migration_generator.rb".freeze, "lib/tasks/clickhouse.rake".freeze]
  s.homepage = "https://github.com/pnixx/clickhouse-activerecord".freeze
  s.licenses = ["MIT".freeze]
  s.rubygems_version = "3.1.4".freeze
  s.summary = "ClickHouse ActiveRecord".freeze

  s.installed_by_version = "3.1.4" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4
  end

  if s.respond_to? :add_runtime_dependency then
    s.add_runtime_dependency(%q<bundler>.freeze, [">= 1.13.4"])
    s.add_runtime_dependency(%q<activerecord>.freeze, [">= 5.2"])
    s.add_development_dependency(%q<bundler>.freeze, ["~> 1.15"])
    s.add_development_dependency(%q<rake>.freeze, ["~> 13.0"])
    s.add_development_dependency(%q<rspec>.freeze, ["~> 3.4"])
    s.add_development_dependency(%q<pry>.freeze, ["~> 0.12"])
  else
    s.add_dependency(%q<bundler>.freeze, [">= 1.13.4"])
    s.add_dependency(%q<activerecord>.freeze, [">= 5.2"])
    s.add_dependency(%q<bundler>.freeze, ["~> 1.15"])
    s.add_dependency(%q<rake>.freeze, ["~> 13.0"])
    s.add_dependency(%q<rspec>.freeze, ["~> 3.4"])
    s.add_dependency(%q<pry>.freeze, ["~> 0.12"])
  end
end
