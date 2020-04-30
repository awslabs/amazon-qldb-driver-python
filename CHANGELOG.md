### Release 2.0.2 (May 7, 2020)
* Added `Getting Started` and `Cookbook` to public api docs.

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