# Clickhouse::Activerecord

A Ruby database ActiveRecord driver for ClickHouse. Support Rails >= 5.0.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'clickhouse-activerecord'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install clickhouse-activerecord

## Usage

Add your `database.yml` connection information with postfix `_clickhouse` for you environment:

```yml
development_clickhouse:
  adapter: clickhouse
  database: database
  host: localhost
  username: username
  password: password
  debug: true # use for showing in to log technical information
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
  host: localhost
  username: username
  password: password
```

Schema dump:

    $ rake clickhouse:schema:dump
    
We use schema for emulate development or tests environment on PostgreSQL adapter.
    
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

NOTE: Creating tables in developing.

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
