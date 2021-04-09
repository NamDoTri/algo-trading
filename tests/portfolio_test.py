from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sequential_test import SequentialTest
from business_logic.models.portfolio import Portfolio
from business_logic.models.stock import Stock
from business_logic.get_data import fetch_portfolio
from database.data_manager.data_access import setup_db, setup_schema

class PortfolioTest(SequentialTest):
    
    def step1_fetch_default_portfolio(self):
        setup_db(drop_old_info=True)
        setup_schema()
        portfolio = fetch_portfolio()
        with self.subTest():
            self.assertIsInstance(portfolio, Portfolio)
        with self.subTest():
            self.assertEqual(portfolio.balance, 4000)
        with self.subTest():
            self.assertEqual(portfolio.current_strategy, 'MACrossover')


    def step2_add_stock(self):
        portfolio = fetch_portfolio()
        stock = Stock('AAPL', 10, 10, datetime.now(), is_testing=True)
        portfolio.add_stock(stock)

        added_stock = portfolio.lst_stocks[0]
        updated_balance = 4000 - 10 * 10

        with self.subTest():
            self.assertEqual(added_stock.symbol, stock.symbol)
        with self.subTest():
            self.assertEqual(portfolio.balance, updated_balance)



    def step3_sell_stock(self):
        portfolio = fetch_portfolio()
        symbol = 'AAPL'
        stock = Stock(symbol, 10, 10, datetime.now(), is_testing=True)
        portfolio.add_stock(stock)

        if len(portfolio.lst_stocks) > 0:
            portfolio.sell_stock(symbol)
            with self.subTest():
                self.assertEqual(portfolio.balance, 4000)
            with self.subTest():
                self.assertEqual(len(portfolio.lst_stocks), 0)
        
if __name__ == '__main__':
    tester = PortfolioTest()
    tester.run_tests()