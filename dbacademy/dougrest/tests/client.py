# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest

from requests import HTTPError

from dbacademy.dougrest import databricks, DatabricksApiException
from dbacademy.dougrest.common import ApiClient


class TestApiClientFeatures(unittest.TestCase):
    """
    Test client error handling, retry, and backoff features.
    """

    def testNotFound(self):
        try:
            databricks.api("GET", "does-not-exist")
            self.fail("404 DatabricksApiException expected")
        except DatabricksApiException as e:
            self.assertEqual(e.http_code, 404)

    def testUnauthorized(self):
        try:
            client = ApiClient(databricks.url, token="INVALID")
            client.api("GET", "2.0/workspace/list")
            self.fail("403 DatabricksApiException expected")
        except DatabricksApiException as e:
            self.assertIn(e.http_code, (401, 403))

    def testThrottle(self):
        client = ApiClient(databricks.url,
                           authorization_header=databricks.session.headers["Authorization"],
                           throttle=2
                           )
        import time
        t1 = time.time()
        client.api_simple("GET", "2.0/clusters/list-node-types")
        t2 = time.time()
        client.api_simple("GET", "2.0/clusters/list-node-types")
        t3 = time.time()
        self.assertLess(t2-t1, 1)
        self.assertGreater(t3-t2, 1)


# COMMAND ----------

def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestApiClientFeatures))
    runner = unittest.TextTestRunner()
    runner.run(suite)


# COMMAND ----------

if __name__ == '__main__':
    main()
