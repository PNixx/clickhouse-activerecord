# frozen_string_literal: true
# coding: utf-8

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require File.expand_path('../lib/clickhouse-activerecord/version', __FILE__)

Gem::Specification.new do |spec|
  spec.name          = 'clickhouse-activerecord'
  spec.version       = ClickhouseActiverecord::VERSION
  spec.authors       = ['Sergey Odintsov']
  spec.email         = ['nixx.dj@gmail.com']

  spec.summary       = 'ClickHouse ActiveRecord'
  spec.description   = 'ActiveRecord adapter for ClickHouse'
  spec.homepage      = 'https://github.com/pnixx/clickhouse-activerecord'
  spec.license       = 'MIT'

  spec.files = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_runtime_dependency 'bundler', '>= 1.13.4'
  spec.add_runtime_dependency 'activerecord', '>= 7.1'

  spec.add_development_dependency 'rake', '~> 13.0'
  spec.add_development_dependency 'rspec', '~> 3.4'
  spec.add_development_dependency 'pry', '~> 0.12'
end
