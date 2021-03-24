import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import yfinance as yf
from MySQLdb.cursors import Cursor
from pandas import DataFrame, MultiIndex
from database.data_manager.data_access import connect_as_user
from database.data_manager.init_queries import insert_daily_price_query
from business_logic.validation import security_exists
from business_logic.insert import insert_city, insert_currency, insert_exchange, insert_security
from business_logic.get_data import fetch_info_of, get_currency_ids, get_exchange_ids, get_security_ids, get_security_symbols

def insert_yf_ticker(new_ticker):
    """
        Parse a ticker from yfinance and insert if not exists
    """ 
    if type(new_ticker) == yf.Ticker:
        symbol = new_ticker.info['symbol']
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

def bulk_insert_yf_daily_price(lst_symbols, dataset, *, data_vendorID, cursor = None):
    """
        data: pd.DataFrame of records of more than 1 symbol
        lst_symbols: list of symbols contained in the DataFrame as strings
    """
    if len(lst_symbols) == 0:
        raise Exception('A list of symbols contained in the dataset must be specified')
    elif not isinstance(dataset, DataFrame):
        raise Exception('Dataset is of unsupported type')
    else:
        if isinstance(dataset.columns, MultiIndex):
            csr = cursor if cursor is Cursor else connect_as_user().cursor()
            lst_db_symbols = get_security_symbols(cursor=csr)
            for symbol in lst_symbols:
                if not (symbol in lst_db_symbols):
                    ticker = yf.Ticker(symbol)
                    insert_yf_ticker(ticker)
                    
                securityID = get_security_ids(f"WHERE abbrev = '{symbol}'")
                data = dataset[symbol]
                insert_yf_daily_price(securityID, data, data_vendorID=data_vendorID)


def insert_yf_daily_price(securityID, data, *, data_vendorID, cursor = None):
    """
        data: pd.DataFrame of only 1 symbol
        data_vendor is default to be Yahoo Finance
    """
    if isinstance(data, DataFrame):
        csr = cursor if cursor is Cursor else connect_as_user().cursor()
        lst_data = list(data.to_records())
        query = insert_daily_price_query(securityID, data_vendorID, '%s', '%s', '%s', '%s', '%s', '%s', '%s')
        csr.executemany(query, lst_data)
    else:
        raise Exception('Dataset is of unsupported type')




