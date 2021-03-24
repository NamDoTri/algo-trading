from multiprocessing import Value
import unittest
import yfinance as yf
from MySQLdb.cursors import Cursor

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from business_logic.yf_insert import insert_yf_ticker, insert_yf_daily_price, bulk_insert_yf_daily_price
from business_logic.get_data import get_exchange_ids, get_security_ids, get_currency_ids
from database.data_manager.data_access import connect_as_user
from db_tests import DbInitializationTest
from helpers.printing import mute_log, unmute_log

class yFinanceInsertTest(unittest.TestCase):
    def setUp(self):
        self._cursor = connect_as_user().cursor()
        mute_log()
        db_reset = DbInitializationTest()
        db_reset.run_tests()
        unmute_log()

    @property
    def cursor(self):
        return self._cursor

    def test_insert_yf_ticker_with_valid_ticker(self):
        symbol = 'AAPL'
        ticker = yf.Ticker(symbol)
        exchange = ticker.info['exchange']
        currency = ticker.info['currency']
        insert_yf_ticker(ticker)

        securityIDs = get_security_ids(f"WHERE abbrev = '{symbol}'")
        exchangeIDs = get_exchange_ids(f"WHERE abbrev = '{exchange}'")
        currencyIDs = get_currency_ids(f"WHERE abbrev = '{currency}'")
        with self.subTest():
            self.assertEqual(len(securityIDs), 1)
        with self.subTest():
            self.assertEqual(len(exchangeIDs), 1)
        with self.subTest():
            self.assertEqual(len(currencyIDs), 1)

    def test_insert_yf_ticker_with_invalid_ticker(self):
        symbol = 'asdlkfjahsdlkfhj'
        ticker = yf.Ticker(symbol)
        self.assertRaises(ValueError, insert_yf_ticker, ticker)

    def test_insert_daily_price(self):
        pass

    def test_bulk_insert_daily_price(self):
        pass

if __name__ == "__main__":
    unittest.main()