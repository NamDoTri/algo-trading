import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from MySQLdb.connections import Connection as DB_Connection

from sequential_test import SequentialTest
from database.data_manager.data_access import setup_schema_queries, setup_db, setup_schema
from database.data_manager.data_access import connect_as_user
from database.memsql.client import connect_as_root

config_path = './db_config.ini'

class DbInitializationTest(SequentialTest):
    def step1_test_root_connection(self):
        connection = connect_as_root(config_path)
        self.assertIsInstance(connection, DB_Connection)

    def step2_test_SU_connection(self):
        setup_db(config_path, drop_old_info=True)
        conn = connect_as_user('algotrader1')
        self.assertIsInstance(conn, DB_Connection)

    def step3_test_create_tables(self):
        connection = connect_as_user('algotrader1', 'algotrading')
        if connection != None:
            cursor = connection.cursor()
            setup_schema(cursor)
            res = cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='algotrading'")
            self.assertEqual(res, len(setup_schema_queries))
        else:
            self.skipTest("No DB connection found")    

if __name__ == "__main__":
    tester = DbInitializationTest()
    tester.run_tests()
