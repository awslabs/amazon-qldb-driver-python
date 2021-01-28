### Release 3.1.0
Add support for obtaining basic server-side statistics on individual statement executions.

#### :tada: Enhancements
* Added `get_consumed_ios` and `get_timing_information` methods in `BufferedCursor` and `StreamCursor` classes to provide server-side execution statistics.
* `get_consumed_ios` returns a dictionary containing the number of read IO requests for a statement execution.
* `get_timing_information` returns a dictionary containing the server side processing time in milliseconds for a statement execution.
* `get_consumed_ios` and `get_timing_information` methods in the `StreamCursor` class are stateful, meaning the statistics returned by them reflect the state at the time of method execution.
* Add `transaction_id` property in `Executor` to provide the Transaction ID if needed.
* The `Config` parameter of `QldbDriver` now appends the `user_agent_extra` value instead of overwriting it.

### Release 3.0.0 (August 20, 2020)
This is a public and generally available(GA) release of the driver, and this version can be used in production applications.

#### Announcements
* The release candidate 2 (v3.0.0rc.2) has been selected as a final release of v3.0.0. No new changes are
introduced between v3.0.0rc.2 and v3.0.0. Please check the [release notes](https://github.com/awslabs/amazon-qldb-driver-python/releases/tag/v3.0.0).

### [Release 3.0.0rc2](https://github.com/awslabs/amazon-qldb-driver-python/releases/tag/v3.0.0rc2) (August 6, 2020)
Note: This version is a release candidate and may not be production ready.

#### Bug Fixes:

* Fixed bug which leads to infinite number of retries when a transaction expires.
* Fixed bug which causes transaction to remain open when an unknown exception is thrown inside execute_lambda.
* Added a limit to the number of times the driver will try to get(from pool)/create a session. 

### [Release 3.0.0-rc.1](https://github.com/awslabs/amazon-qldb-driver-python/releases/tag/v3.0.0-rc.1) (June 22, 2020)
Note: This version is a release candidate and may not be production ready.

#### Breaking changes:

* [(#23)](https://github.com/awslabs/amazon-qldb-driver-python/issues/23) Moved Session pooling functionality to 
`QldbDriver` and removed `PooledQldbDriver`.
* [(#28)](https://github.com/awslabs/amazon-qldb-driver-python/issues/28) Removed interfaces which allow developers to 
get a session from the pool and execute transaction.
* [(#29)](https://github.com/awslabs/amazon-qldb-driver-python/issues/29) Renamed `QldbDriver` property `pool_limit` to 
`max_concurrent_transactions`.
* [(#30)](https://github.com/awslabs/amazon-qldb-driver-python/issues/30) Removed `QldbDriver` property `pool_timeout`.
* [(#31)](https://github.com/awslabs/amazon-qldb-driver-python/issues/31) Moved method `list_tables` to the driver 
instance
* [(#27)](https://github.com/awslabs/amazon-qldb-driver-python/issues/27) Removed `retry_indicator` 
from `QldbDriver.execute_lambda`.
* [(#27)](https://github.com/awslabs/amazon-qldb-driver-python/issues/27) Moved `retry_limit` from `QldbDriver` to 
`RetryConfig`.
 [(#34)](https://github.com/awslabs/amazon-qldb-driver-python/issues/34)  Removed `QldbDriver.execute_statement`.

#### New features:

* [(#27)](https://github.com/awslabs/amazon-qldb-driver-python/issues/27) Added support for defining custom retry 
backoffs.

#### Announcements 

* Dropping support for Python v3.4 and v3.5. Going forward the minimum Python version required will be v3.6 for Pyqldb 3.x 
series.


### Release 2.0.2 (May 4, 2020)
* Added `Getting Started` and `Cookbook` to public API docs.

### Release 2.0.1 (March 18, 2020)

* Fixed README to reflect the correct minimum python version required.
The driver requires Python 3.4 or later, earlier it was mentioned to be 3.x

### Release 2.0.0 (March 11, 2020)

#### New features:
* Added Execute methods to PooledQldbDriver
* Added support for python native types for [execute_statement](https://amazon-qldb-driver-python.readthedocs.io/en/v2.0.0/reference/session/pooled_qldb_session.html#pyqldb.session.pooled_qldb_session.PooledQldbSession.execute_statement) parameters

#### Unavoidable breaking changes:
* In order to be more pythonic, the method signature of [execute_statement](https://amazon-qldb-driver-python.readthedocs.io/en/v2.0.0/reference/session/pooled_qldb_session.html#pyqldb.session.pooled_qldb_session.PooledQldbSession.execute_statement) has 
been changed to receive *args. This is a breaking change for any application 
that uses 1.0.0-rc.2 version of the driver. Starting v2.0, applications should 
pass execute_statement parameters as comma separated arguments instead of passing them as a list.

### Release 1.0.0-rc.2 (October 29, 2019)

* Fixes for small documentation issues.

### Release 1.0.0-rc.1 (October 28, 2019)

* Initial preview release of the Amazon QLDB Driver for Python.
