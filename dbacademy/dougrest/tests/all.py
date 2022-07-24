def main():
    import unittest
    from dbacademy.dbrest.tests.highlevel import TestHighLevelFeatures
    from dbacademy.dougrest.tests.client import TestApiClientFeatures
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestHighLevelFeatures))
    suite.addTest(unittest.makeSuite(TestApiClientFeatures))
    runner = unittest.TextTestRunner()
    runner.run(suite)


if __name__ == '__main__':
    main()
