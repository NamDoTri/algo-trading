import unittest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.mongo_client import get_mongo_db_conn
from pymongo.database import Database

class MongoDBConnectionTest(unittest.TestCase):
    def test_connection(self):
        db_conn = get_mongo_db_conn()
        self.assertIsInstance(db_conn, Database)

if __name__ == '__main__':
    unittest.main()