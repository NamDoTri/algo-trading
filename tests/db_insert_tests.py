import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from MySQLdb.cursors import Cursor
from sequential_test import SequentialTest
from database.data_manager.data_access import connect_as_user
from database.data_manager.data_access import insert_exchange, insert_security, insert_daily_price, insert_data_vendor, insert_currency, insert_city, insert_country
from database.data_manager.init_queries import country_table_name, city_table_name, currency_table_name, data_vendor_table_name, exchange_table_name, securities_table_name, daily_price_table_name
from database.data_manager.data_access import delete_exchange

class DBInsertTests(SequentialTest):
    def step1_insert_country(self, cursor):
        insert_country(cursor, "USA", "United State of America")
        cursor.execute(f"SELECT * FROM {country_table_name} WHERE abbrev='USA'")
        res = cursor.fetchone()
        if not (res is None):
            self.assertEqual(res[2], "United State of America")
        else:
            self.fail()

    def step2_insert_city(self, cursor):
        insert_city(cursor, 'NY', 'New York', 1)
        cursor.execute(f"SELECT * FROM {city_table_name} WHERE city_name='New York'")
        res = cursor.fetchone()
        self.assertIsNotNone(res)

    def step3_insert_currency(self, cursor):
        insert_currency(cursor, 'USD', 'US Dollar')
        cursor.execute(f"SELECT * FROM {currency_table_name} WHERE abbrev='USD'")
        res = cursor.fetchone()
        if not (res is None):
            self.assertEqual(res[2], "US Dollar")
        else:
            self.fail()
    
    def step4_insert_data_vendor(self, cursor):
        insert_data_vendor(cursor, "Yahoo Finance", "finance.yahoo.com")
        cursor.execute(f"SELECT * FROM {data_vendor_table_name} WHERE vendor_name='Yahoo Finance'")
        res = cursor.fetchone()
        if not (res is None):
            self.assertEqual(res[2], "finance.yahoo.com")
        else:
            self.fail()

    def step5_insert_exchange(self, cursor):
        insert_exchange(cursor, 'NYSE', exchange_name='New York Stock Exchange', cityID=1)
        cursor.execute(f"SELECT * FROM {exchange_table_name} WHERE abbrev='NYSE'")
        res = cursor.fetchone()
        if not (res is None):
            self.assertEqual(res[2], "New York Stock Exchange")
        else:
            self.fail()

    def step6_insert_security(self, cursor):
        insert_security(cursor, 1, 'S&P 500', security_name="Standard & Poor's 500 Index", currencyID=1)
        cursor.execute(f"SELECT * FROM {securities_table_name} WHERE abbrev='S&P 500'")
        res = cursor.fetchone()
        if not (res is None):
            self.assertEqual(res[3], "Standard & Poor`s 500 Index")
        else:
            self.fail()

    def step7_insert_daily_price(self, cursor):
        insert_daily_price(cursor, 1, 1, 1.0, 1.0, 1.0, 1.0, 1.0)
        cursor.execute(f"SELECT * FROM {daily_price_table_name} WHERE securityID=1")
        res = cursor.fetchone()
        if not (res is None):
            self.assertEqual(res[3], float(1))
        else:
            self.fail()

    def step8_delete_exchange(self, cursor):
        delete_exchange(cursor, lst_IDs=[1])
        is_passed = True

        cursor.execute(f"SELECT * FROM {exchange_table_name} WHERE ID=1")
        if cursor.fetchone() != None:
            is_passed = False

        if is_passed:
            cursor.execute(f"SELECT * FROM {securities_table_name} WHERE ID=1")
            if cursor.fetchone() != None: is_passed = False

        if is_passed: 
            cursor.execute(f"SELECT * FROM {daily_price_table_name} WHERE securityID=1")
            if cursor.fetchone() != None: is_passed = False

        self.assertTrue(is_passed)

    def run_tests(self, cursor):
        if not isinstance(cursor, Cursor): raise Exception("DB cursor not found.")

        for name, step in self._steps():
            try:
                step(cursor)
                print('Test {} passed'.format(name))
            except Exception as e:
                self.fail("{} failed ({}: {})".format(step, type(e), e))
        print('All tests passed')

if __name__ == "__main__":
    conn = connect_as_user()
    cursor = conn.cursor()
    tester = DBInsertTests()
    tester.run_tests(cursor)