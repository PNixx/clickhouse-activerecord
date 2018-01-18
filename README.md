# Clickhouse::Activerecord

A Ruby database ActiveRecord driver for ClickHouse.

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

Or global connection, but schema dump don't works:

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

## Donations

Donations to this project are going directly to [PNixx](https://github.com/PNixx), the original author of this project:

* BTC address: `3Dny9T9PTVYoQhnJxgZorRRNycYYiPrYMA`
* ETH address: `0x7eec67c120828334d0e802ce46fc4895f2b386c4`
* XMR address: `42gP71qLB5M43RuDnrQ3vSJFFxis9Kw9VMURhpx9NLQRRwNvaZRjm2TFojAMC8Fk1BQhZNKyWhoyJSn5Ak9kppgZPjE17Zh`

## Contributing

Bug reports and pull requests are welcome on GitHub at [https://github.com/pnixx/clickhouse-activerecord](https://github.com/pnixx/clickhouse-activerecord). This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
