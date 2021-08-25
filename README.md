# Clickhouse::Activerecord

A Ruby database ActiveRecord driver for ClickHouse. Support Rails >= 5.2.
Support ClickHouse version from 20.9 LTS.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'clickhouse-activerecord'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install clickhouse-activerecord
    
## Available database connection parameters
```yml
default: &default
  adapter: clickhouse
  database: database
  host: localhost
  port: 8123
  username: username
  password: password
  ssl: true # optional for using ssl connection
  debug: true # use for showing in to log technical information
  migrations_paths: db/clickhouse # optional, default: db/migrate_clickhouse
  cluster_name: 'cluster_name' # optional for creating tables in cluster 
  replica_name: '{replica}' # replica macros name, optional for creating replicated tables
```

Alternatively if you wish to pass a custom `Net::HTTP` transport (or any other
object which supports a `.post()` function with the same parameters as
`Net::HTTP`'s), you can do this directly instead of specifying
`host`/`port`/`ssl`:

```ruby
class ActionView < ActiveRecord::Base
  establish_connection(
    adapter: 'clickhouse',
    database: 'database',
    connection: Net::HTTP.start('http://example.org', 8123)
  )
end
```

## Usage in Rails 5

Add your `database.yml` connection information with postfix `_clickhouse` for you environment:

```yml
development_clickhouse:
  adapter: clickhouse
  database: database
```

Add to your model:

```ruby
class Action < ActiveRecord::Base
  establish_connection "#{Rails.env}_clickhouse".to_sym
end
```

For materialized view model add:
```ruby
class ActionView < ActiveRecord::Base
  establish_connection "#{Rails.env}_clickhouse".to_sym
  self.is_view = true
end
```

Or global connection:

```yml
development:
  adapter: clickhouse
  database: database
```

## Usage in Rails 6 with second database

Add your `database.yml` connection information for you environment:

```yml
development:
  primary:
    ...
    
  clickhouse:
    adapter: clickhouse
    database: database
```

Connection [Multiple Databases with Active Record](https://guides.rubyonrails.org/active_record_multiple_databases.html) or short example:

```ruby
class Action < ActiveRecord::Base
  connects_to database: { writing: :clickhouse, reading: :clickhouse }
end
```

### Rake tasks

**Note!** For Rails 6 you can use default rake tasks if you configure `migrations_paths` in your `database.yml`, for example: `rake db:migrate`

Create / drop / purge / reset database:
 
    $ rake clickhouse:create
    $ rake clickhouse:drop
    $ rake clickhouse:purge
    $ rake clickhouse:reset

Prepare system tables for rails:

    $ rake clickhouse:prepare_schema_migration_table
    $ rake clickhouse:prepare_internal_metadata_table
    
Migration:

    $ rails g clickhouse_migration MIGRATION_NAME COLUMNS
    $ rake clickhouse:migrate
    $ rake clickhouse:rollback

### Dump / Load for multiple using databases

If you using multiple databases, for example: PostgreSQL, Clickhouse.

Schema dump to `db/clickhouse_schema.rb` file:

    $ rake clickhouse:schema:dump
    
Schema load from `db/clickhouse_schema.rb` file:

    $ rake clickhouse:schema:load

For export schema to PostgreSQL, you need use:

    $ rake clickhouse:schema:dump -- --simple

Schema will be dump to `db/clickhouse_schema_simple.rb`. If default file exists, it will be auto update after migration.
    
Structure dump to `db/clickhouse_structure.sql` file:

    $ rake clickhouse:structure:dump
    
Structure load from `db/clickhouse_structure.sql` file:

    $ rake clickhouse:structure:load

### Dump / Load for only Clickhouse database using

    $ rake db:schema:dump  
    $ rake db:schema:load  
    $ rake db:structure:dump  
    $ rake db:structure:load  
    
### Insert and select data

```ruby
Action.where(url: 'http://example.com', date: Date.current).where.not(name: nil).order(created_at: :desc).limit(10)
# Clickhouse Action Load (10.3ms)  SELECT  actions.* FROM actions WHERE actions.date = '2017-11-29' AND actions.url = 'http://example.com' AND (actions.name IS NOT NULL)  ORDER BY actions.created_at DESC LIMIT 10
#=> #<ActiveRecord::Relation [#<Action *** >]>

Action.create(url: 'http://example.com', date: Date.yesterday)
# Clickhouse Action Load (10.8ms)  INSERT INTO actions (url, date) VALUES ('http://example.com', '2017-11-28')
#=> true
 
ActionView.maximum(:date)
# Clickhouse (10.3ms)  SELECT maxMerge(actions.date) FROM actions
#=> 'Wed, 29 Nov 2017'
```


### Migration Data Types

Integer types are unsigned by default. Specify signed values with `:unsigned =>
false`. The default integer is `UInt32`

| Type (bit size)    | Range | :limit (byte size) |
| :---        |    :----:   |          ---: |
| Int8 | -128 to 127 | 1 | 
| Int16 | -32768 to 32767 | 2 |
| Int32 | -2147483648 to 2,147,483,647 | 3,4 |
| Int64 | -9223372036854775808 to 9223372036854775807] |  5,6,7,8 |
| Int128 | ... | 9 - 15 |
| Int256 | ... | 16+ |
| UInt8 | 0 to 255 | 1 |
| UInt16 | 0 to 65,535 | 2 |
| UInt32 | 0 to 4,294,967,295 | 3,4 |
| UInt64 | 0 to 18446744073709551615 | 5,6,7,8 |
| UInt256 | 0 to ... | 8+ |
| Array | ... | ... |

Example:

``` ruby
class CreateDataItems < ActiveRecord::Migration
  def change
    create_table "data_items", id: false, options: "VersionedCollapsingMergeTree(sign, version) PARTITION BY toYYYYMM(day) ORDER BY category", force: :cascade do |t|
      t.date "day", null: false
      t.string "category", null: false
      t.integer "value_in", null: false
      t.integer "sign", limit: 1, unsigned: false, default: -> { "CAST(1, 'Int8')" }, null: false
      t.integer "version", limit: 8, default: -> { "CAST(toUnixTimestamp(now()), 'UInt64')" }, null: false
    end
  end
end
      
```


### Using replica and cluster params in connection parameters

```yml
default: &default
  ***
  cluster_name: 'cluster_name'
  replica_name: '{replica}'
```

`ON CLUSTER cluster_name` will be attach to all queries create / drop.

Engines `MergeTree` and all support replication engines will be replaced to `Replicated***('/clickhouse/tables/cluster_name/database.table', '{replica}')`

## Donations

Donations to this project are going directly to [PNixx](https://github.com/PNixx), the original author of this project:

* BTC address: `1H3rhpf7WEF5JmMZ3PVFMQc7Hm29THgUfN`
* ETH address: `0x6F094365A70fe7836A633d2eE80A1FA9758234d5`
* XMR address: `42gP71qLB5M43RuDnrQ3vSJFFxis9Kw9VMURhpx9NLQRRwNvaZRjm2TFojAMC8Fk1BQhZNKyWhoyJSn5Ak9kppgZPjE17Zh`

## Development

After checking out the repo, run `bin/setup` to install dependencies. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at [https://github.com/pnixx/clickhouse-activerecord](https://github.com/pnixx/clickhouse-activerecord). This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
