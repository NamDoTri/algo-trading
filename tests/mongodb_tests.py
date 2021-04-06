import unittest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pymongo.collection import Collection
from database.mongo_client import get_mongo_db_conn, reset_db
from business_logic.model_crud import save_model_to_mongo, load_saved_model_from_mongo

class MongoDBConnectionTest(unittest.TestCase):
    MODEL_NAME = 'TestModel'

    def setUp(self):
        reset_db()

    def test1_connection(self):
        db_conn = get_mongo_db_conn()
        self.assertIsInstance(db_conn, Collection)

    def test2_saving_model(self):
        conn = get_mongo_db_conn()
        new_ID = save_model_to_mongo('Test string as object', self.MODEL_NAME, conn)
        res = conn.find_one({'model_name': self.MODEL_NAME})
        saved_ID = res['_id']

        self.assertEqual(new_ID, saved_ID)

    def test3_loading_model(self):
        conn = get_mongo_db_conn()
        save_model_to_mongo('Test string as object', self.MODEL_NAME, conn)
        res = load_saved_model_from_mongo(self.MODEL_NAME)
        self.assertIsInstance(res, str)



if __name__ == '__main__':
    unittest.main()