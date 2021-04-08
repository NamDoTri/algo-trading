from os.path import expanduser
import pickle
from .stock import Stock
from multipledispatch import dispatch
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from business_logic.validation import ticker_is_valid

class Portfolio:
    balance = 4000
    current_strategy = ''
    num_stocks = 5
    lst_stocks = [] # holds 5 stocks
    max_per_stock = balance/num_stocks

    def __init__(self) -> None:
        pass

    @dispatch(str)
    def add_stock(self, value):
        '''
            Parse serialized stock from DB and add to this portfolio
            value: string of serialized stock
        '''
        stock = pickle.loads(value)
        if isinstance(stock, Stock):
            self.add_stock(stock)
        else:
            raise ValueError('Parsed object is not of type Stock.')

    @dispatch(Stock)
    def add_stock(self, stock):
        '''
            Add a new yfinance.Ticker to this portfolio
        '''
        # add stock
        if isinstance(stock, Stock):
            if len(self.lst_stocks) < 5:
                if stock.total_worth() <= self.max_per_stock:
                    self.lst_stocks.append(stock)
                else:
                    raise Exception('The total sum of this stock exceeds the amount of balance allocated for it.')
            else:
                raise Exception('A portfolio can only holds 5 stocks at a time.')
        else:
            raise TypeError('Parameter stock must be of type Stock.')