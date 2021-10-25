### Version 0.5.6 (Oct 25, 2021)
 
 * Added auto creating service distributed tables and additional options for creating view [@ygreeek](https://github.com/ygreeek)
 * Added default user agent

### Version 0.5.3 (Sep 22, 2021)
 
 * Fix replica cluster for a new syntax MergeTree
 * Fix support rails 5.2 on alter table
 * Support array type of column
 * Support Rails 6.1.0 [@bdevel](https://github.com/bdevel)

### Version 0.4.10 (Mar 10, 2021)

 * Support ClickHouse 20.9+
 * Fix schema create / dump
 * Support all integer types through :limit and :unsigned [@bdevel](https://github.com/bdevel)

### Version 0.4.4 (Sep 23, 2020)

 * Full support migration and rollback database
 * Support cluster and replica. Auto inject to SQL queries.
 * Fix schema dump/load
 * Can dump schema for using PostgreSQL

### Version 0.3.10 (Dec 20, 2019)

 * Support structure dump/load [@StoneGod](https://github.com/StoneGod)

### Version 0.3.6 (Sep 2, 2019)

 * Support Rails 6.0
 * Fix relation `last` method

### Version 0.3.4 (Jun 28, 2019)

 * Fix DateTime sql format without microseconds for Rails 5.2
 * Support ssl connection
 * Migration support
 * Rake tasks for create / drop database

### Version 0.3.0 (Nov 27, 2018)

 * Support materialized view
 * Aggregated functions for view
 * Schema dumper with SQL create table
 * Added migrations support [@Bugagazavr](https://github.com/Bugagazavr)

### Version 0.2.0 (Oct 3, 2017)

 * Support Rails 5.0

### Version 0.1.2 (Sep 27, 2017)

 * Fix Big Int type

### Version 0.1.0 (Aug 31, 2017)

 * Initial release
