import pickle
import re
from multipledispatch import dispatch
from mysql.connector.cursor import CursorBase
from .stock import Stock
from database.data_manager.data_access import connect_as_user
from database.data_manager.init_queries import metadata_table_name
from database.mongo_client import get_mongo_db_conn, get_remote_client

class Portfolio:
    @property
    def lst_symbols(self):
        return [stock.symbol for stock in self.lst_stocks if len(self.lst_stocks) > 0]

    def __init__(self):
        self.balance = 4000
        self.current_strategy = ''
        self.num_stocks = 5
        self.lst_stocks = [] # holds 5 stocks max
        self.max_per_stock = self.balance/self.num_stocks

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
        # TODO: implement ignore_balance_check ?
        if isinstance(stock, Stock):
            if stock not in self:
                if len(self.lst_stocks) <= 5:
                    if stock.total_worth() <= self.max_per_stock:
                        self.lst_stocks.append(stock)
                        self.balance -= stock.total_worth()
                    else:
                        raise Exception(f'Total worth of {stock.symbol} exceeds what is allocated for it.')
                else:
                    raise Exception('A portfolio can only holds 5 stocks at a time.')
            else:
                raise ValueError(f'Portfolio already has stock with symbol {stock.symbol}')
        else:
            raise TypeError('Parameter stock must be of type Stock.')

    def drop_stock(self, symbol) -> Stock:
        if isinstance(symbol, str):
            try:
                stock = next(s for s in self.lst_stocks if s.symbol == symbol)
                balance_gain = stock.total_worth()
                self.balance += balance_gain
                self.lst_stocks.remove(stock)
                return stock
            except StopIteration:
                raise ValueError(f"Portfolio doesn't contain stock with symbol {symbol}.")
        else:
            raise TypeError('symbol parameter must be a string.')

    def is_using_ML(self) -> bool:
        pattern = re.compile('^ML_')
        res = pattern.match(self.current_strategy)
        return res is not None        

    def save_portfolio(self, db_cursor = None):
        cursor = db_cursor # if isinstance(db_cursor, CursorBase) else connect_as_user().cursor()
        query = f"UPDATE {metadata_table_name} SET val = %s WHERE field = %s"
        lst_params = [] 
        lst_params.append(('balance', str(self.balance)))
        lst_params.append(('current_strategy', self.current_strategy))
        
        cursor.executemany(query, lst_params)
        self.save_stocks()

    def save_stocks(self, drop_old_entries = True):
        # conn = get_mongo_db_conn('owned_stocks')
        conn = get_remote_client('owned_stocks')

        if drop_old_entries:
            conn.drop()

        for stock in self.lst_stocks:
            serialized_stock = pickle.dumps(stock)
            conn.insert_one({'stock_bin': serialized_stock}) 

    def get_stock(self, symbol) -> Stock:
        return next((stock for stock in self.lst_stocks if stock.symbol == symbol), None)

    def __contains__(self, stock):
        if isinstance(stock, Stock):
            return stock.symbol in [stock.symbol for stock in self.lst_stocks]
        else:
            return False
