### Version 1.0.7 (Apr 27, 2024)

* Support table indexes
* Fix non-canonical UUID by [@PauloMiranda98](https://github.com/PauloMiranda98) in (#117)
* Fix precision loss due to JSON float parsing by [@jenskdsgn](https://github.com/jenskdsgn) in (#129)
* Support functions by [@felix-dumit](https://github.com/felix-dumit) in (#120)
* Hotfix/rails71 change column by [@trumenov](https://github.com/trumenov) in (#132)
* Fix DB tasks

### Version 1.0.5 (Mar 14, 2024)

* GitHub workflows
* Fix injection internal and schema classes for rails 7
* Add support for binary string by [@PauloMiranda98](https://github.com/PauloMiranda98) in (#116)

### Version 1.0.4 (Feb 2, 2024)

* Use ILIKE for `model.arel_table[:column]#matches` by [@stympy](https://github.com/stympy) in (#115)
* Fixed `insert_all` for array column (#71)
* Register Bool and UUID in type map by [@lukinski](https://github.com/lukinski) in (#110)
* Refactoring `final` method
* Support update & delete for clickhouse from version 23.3 and newer (#93)

### Version 1.0.0 (Nov 29, 2023)

 * Full support Rails 7.1+
 * Full support primary or multiple databases

### Version 0.6.0 (Oct 19, 2023)

 * Added `Bool` column type instead `Uint8` (#78). Supports ClickHouse 22+ database only
 * Added `final` method (#81) (The `ar_internal_metadata` table needs to be deleted after a gem update)
 * Added `settings` method (#82)
 * Fixed convert aggregation type (#92)
 * Fixed raise error not database exist (#91)
 * Fixed internal metadata update (#84)

### Version 0.5.10 (Jun 22, 2022)

 * Fixes to create_table method (#70)
 * Added support for rails 7 (#65)
 * Use ClickHouse default KeepAlive timeout of 10 seconds (#67)

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
