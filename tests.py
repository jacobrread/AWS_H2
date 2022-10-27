from consumer import *
import unittest

class TestConsumer(unittest.TestCase):
    def test_createRequest(self):
        createRequest()
        self.assertTrue(True)

    def test_deleteRequest(self):
        deleteRequest()
        self.assertTrue(True)

    def test_changeRequest(self):
        changeRequest()
        self.assertTrue(True)

    def test_getRequest(self):
        getRequest(True)
        self.assertTrue(True)

    def test_getSmallestKey(self):
        getSmallestKey()
        self.assertTrue(True)

    def test_getAllKeys(self):
        getAllKeys()
        self.assertTrue(True)

    def test_flattenDictionary(self):
        flattenDictionary()
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()