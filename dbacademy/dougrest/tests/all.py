def main():
    import unittest
    from dbacademy.dougrest.tests.highlevel import TestHighLevelFeatures
    from dbacademy.dougrest.tests.client import TestApiClient
    from dbacademy.dougrest.tests.accounts import TestAccountsApi
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestHighLevelFeatures))
    suite.addTest(unittest.makeSuite(TestApiClient))
    suite.addTest(unittest.makeSuite(TestAccountsApi))
    runner = unittest.TextTestRunner()
    runner.run(suite)


if __name__ == '__main__':
    main()
