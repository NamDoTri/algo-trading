import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import yfinance as yf
from MySQLdb.cursors import Cursor
from pandas import DataFrame, MultiIndex
from pandas.core.indexes.datetimes import DatetimeIndex
from database.data_manager.data_access import connect_as_user
from database.data_manager.init_queries import insert_daily_price_query
from business_logic.validation import security_exists
from business_logic.insert import insert_currency, insert_exchange, insert_security
from business_logic.get_data import get_currency_ids, get_exchange_ids, get_security_ids, get_security_symbols

def insert_yf_ticker(new_ticker):
    """
        Parse a ticker from yfinance and insert if not exists
    """ 
    if type(new_ticker) == yf.Ticker:
        try:
            symbol = new_ticker.info['symbol']
        except ValueError as e:
            raise e
            
        if not security_exists(security_abbrev=symbol):
            conn = connect_as_user()
            cursor = conn.cursor()

            # insert if not exists
            currency = new_ticker.info['currency']
            exchange = new_ticker.info['exchange']
            insert_currency(cursor, currency)
            insert_exchange(cursor, exchange)

            exchangeID = get_exchange_ids(f"WHERE abbrev = '{exchange}'")[0]
            currencyID = get_currency_ids(f"WHERE abbrev = '{currency}'")[0]
            insert_security(cursor, exchangeID, symbol, currencyID=currencyID)
    else:
        raise Exception('Method requires object of class yfinance.Ticker.')

def bulk_insert_yf_daily_price(lst_symbols, dataset, *, data_vendorID, cursor = None) -> int:
    """
        data: pd.DataFrame of records of more than 1 symbol
        lst_symbols: list of symbols contained in the DataFrame as strings
        Returns number of rows affected
    """
    if len(lst_symbols) == 0:
        raise Exception('A list of symbols contained in the dataset must be specified')
    elif not isinstance(dataset, DataFrame):
        raise Exception('Dataset is of unsupported type')
    else:
        if isinstance(dataset.columns, MultiIndex):
            csr = cursor if cursor is Cursor else connect_as_user().cursor()
            lst_db_symbols = get_security_symbols(cursor=csr)
            number_of_rows = 0

            for symbol in lst_symbols:
                if not (symbol in lst_db_symbols):
                    ticker = yf.Ticker(symbol)
                    insert_yf_ticker(ticker)
                    
                securityID = get_security_ids(f"WHERE abbrev = '{symbol}'")[0]
                data = dataset[symbol]
                prep_data = parse_price_df(data)
                number_of_rows += insert_yf_daily_price(securityID, prep_data, data_vendorID=data_vendorID)

            return number_of_rows
        else:
            raise Exception('Unable to parse data from DataFrame')


def insert_yf_daily_price(securityID, data, *, data_vendorID = 0, cursor = None) -> int:
    """
        data: pd.DataFrame of only 1 symbol
        data_vendor is default to be Yahoo Finance
    """
    if isinstance(data, list):
        if len(data) > 0:
            if len(data[0]) == 6 :
                csr = cursor if cursor is Cursor else connect_as_user().cursor()
                query = insert_daily_price_query(securityID, data_vendorID, '%s', '%s', '%s', '%s', '%s', '%s')
                number_of_rows = csr.executemany(query, data)
                return number_of_rows
            else: 
                raise Exception('Number of columns is not correct.')
        else:
            raise Exception('Cannot insert an empty dataset.')
    else:
        raise Exception('Dataset is of unsupported type.')

def parse_price_df(orig_data) -> list:
    """
        Parse DataFrame of security price from yfinance
    """
    if isinstance(orig_data, DataFrame):
        if (orig_data.shape[1] >= 5) and isinstance(orig_data.index, DatetimeIndex):
            data = orig_data.copy()
            lst_necessary_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            data = data[lst_necessary_cols]
            data.index = data.index.strftime('%d-%m-%Y')
            return data.to_records(index=True).tolist()
        else:
            raise Exception("DataFrame is not in a correct shape")
    else:
        raise Exception("Input is not a DataFrame")


