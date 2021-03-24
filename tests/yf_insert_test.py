import unittest
import yfinance as yf

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from business_logic.yf_insert import insert_yf_ticker, insert_yf_daily_price, bulk_insert_yf_daily_price, parse_price_df
from business_logic.get_data import get_exchange_ids, get_security_ids, get_currency_ids, get_security_symbols
from database.data_manager.data_access import connect_as_user
from db_tests import DbInitializationTest
from helpers.printing import mute_log, unmute_log

class yFinanceInsertTest(unittest.TestCase):
    def setUp(self):
        print('setUp method called.')
        self._cursor = connect_as_user().cursor()
        mute_log()
        db_reset = DbInitializationTest()
        db_reset.run_tests()
        unmute_log()

        self._ticker = yf.Ticker('AAPL')

        if self._testMethodName == self.test_insert_daily_price.__name__:
            insert_yf_ticker(self._ticker)

    @property
    def cursor(self):
        return self._cursor

    def test_insert_yf_ticker_with_valid_ticker(self):
        ticker = self._ticker
        symbol = self._ticker
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
        securityID = get_security_ids("WHERE abbrev = 'AAPL'")[0]
        data = self._ticker.history(period='1mo')
        data = parse_price_df(data) # -> a list
        num_affected_rows = insert_yf_daily_price(securityID, data)
        self.assertEqual(num_affected_rows, len(data))

    def test_bulk_insert_daily_price(self):
        pass

if __name__ == "__main__":
    unittest.main()