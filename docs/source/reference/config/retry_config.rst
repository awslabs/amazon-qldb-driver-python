=====================
RetryConfig Reference
=====================


.. automodule:: pyqldb.config.retry_config
   :members:
   :undoc-members:

Usage
-------------------
.. code-block:: python


    from pyqldb.config.retry_config import RetryConfig
    from pyqldb.driver.qldb_driver import QldbDriver

    # Configuring Retry limit to 2
    retry_config = RetryConfig(retry_limit=2)
    qldb_driver = QldbDriver("test-ledger", retry_config=retry_config)

    # Configuring a custom back off which increases delay by 1s for each attempt.

    def custom_backoff(retry_attempt, error, transaction_id):
        return 1000 * retry_attempt

    retry_config_custom_backoff = RetryConfig(retry_limit=2, custom_backoff=custom_backoff)
    qldb_driver = QldbDriver("test-ledger", retry_config=retry_config_custom_backoff)