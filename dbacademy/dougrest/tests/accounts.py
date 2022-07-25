# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest

from dbacademy.dougrest import AccountsApi


class TestAccountsApi(unittest.TestCase):
    """
    General test of API connectivity for each of the main Databricks Workspace Rest APIs.
    """

    def testListWorkspaces(self):
        accounts = AccountsApi("b6e87bd6-c65c-46d3-abde-6840bf706d39",
                               user="class+curriculum@databricks.com",
                               password="REDACTED")
        result = accounts.workspaces.list()
        self.assertIsInstance(result, list)


# COMMAND ----------

def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestHighLevelFeatures))
    runner = unittest.TextTestRunner()
    runner.run(suite)


# COMMAND ----------

if __name__ == '__main__':
    main()
