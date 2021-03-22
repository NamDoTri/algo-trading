import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import yfinance as yf
from database.data_manager.data_access import connect_as_user
from business_logic.validation import security_exists
from business_logic.insert import insert_currency, insert_exchange, insert_security
from business_logic.get_data import get_currency_ids, get_exchange_ids

def insert_yf_ticker(new_ticker):
    """
        Parse a ticker from yfinance and insert if not exists
    """ 
    if type(new_ticker) == yf.Ticker:
        symbol = new_ticker.info['symbol']
        if not security_exists(security_abbrev=symbol):
            conn = connect_as_user()
            cursor = conn.cursor()

            currency = new_ticker.info['currency']
            exchange = new_ticker.info['exchange']
            exchangeID = get_exchange_ids(f"WHERE abbrev = '{exchange}'")[0]
            currencyID = get_currency_ids(f"WHERE abbrev = '{currency}'")[0]

            insert_currency(cursor, currency)
            insert_exchange(cursor, exchange)
            insert_security(cursor, exchangeID, symbol, currencyID=currencyID)
    else:
        raise Exception('Method requires object of class yfinance.Ticker.')

def insert_yf_daily_price(data):
    """
        data: pd.DataFrame of records
    """

