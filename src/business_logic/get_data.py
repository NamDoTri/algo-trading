import pickle
import yfinance as yf
from pymongo.collection import Collection
from pandas import DataFrame
from MySQLdb.cursors import Cursor, DictCursor
from database.data_manager.init_queries import exchange_table_name, securities_table_name, currency_table_name, city_table_name, country_table_name, data_vendor_table_name, metadata_table_name
from database.data_manager.data_access import connect_as_user
from database.mongo_client import get_mongo_db_conn
from database.mongo_client import get_remote_client
from business_logic.decision_making.data_prepration import get_OHLC_df, label_OHLC_df
from .models.portfolio import Portfolio

# REGION FROM DATABASE
def get_country_ids(cursor=None):
    csr = cursor if cursor is Cursor else connect_as_user().cursor()
    if csr:
        csr.execute(f'SELECT ID FROM {country_table_name}')
        rows = csr.fetchall()
        lst_countries = [row[0] for row in rows]
        return lst_countries
    else:
        raise Exception(get_country_ids.__name__ + '  Cannot connect to the database to verify country name.') 

def get_city_ids(cursor=None):
    csr = cursor if cursor is Cursor else connect_as_user().cursor()
    if csr:
        csr.execute(f'SELECT ID FROM {city_table_name}')
        rows = csr.fetchall()
        lst_cities = [row[0] for row in rows]
        if not (0 in lst_cities): lst_cities.append(0)
        return lst_cities
    else:
        raise Exception(get_city_ids.__name__ + '  Cannot connect to the database to verify city name.') 

def get_exchange_ids(condition='', *, cursor = None) -> list:
    csr = cursor if cursor is Cursor else connect_as_user().cursor()
    if csr:
        query = f'SELECT ID FROM {exchange_table_name} ' + condition
        csr.execute(query)
        rows = csr.fetchall()
        lst_exchanges = [row[0] for row in rows]
        return lst_exchanges
    else:
        raise Exception(get_exchange_ids.__name__ + '  Cannot connect to the database to verify exchange.')

def get_security_ids(condition = '', *, cursor=None):
    csr = cursor if cursor is Cursor else connect_as_user().cursor()
    if csr:
        query = f'SELECT ID FROM {securities_table_name} ' + condition
        csr.execute(query)
        rows = csr.fetchall()
        lst_security = [row[0] for row in rows]
        return lst_security
    else:
        raise Exception(get_security_ids.__name__ + '  Cannot connect to the database to verify security.')

def get_security_symbols(condition = '', *,cursor= None) -> list:
    csr = cursor if cursor is Cursor else connect_as_user().cursor()
    if csr:
        query = f'SELECT abbrev FROM {securities_table_name} ' + condition
        csr.execute(query)
        rows = csr.fetchall()
        lst_symbols = [row[0] for row in rows]
        return lst_symbols
    else:
        raise Exception(get_security_symbols.__name__ + '  Cannot connect to the database to get security data.')

def get_currency_ids(condition='', *, cursor=None):
    csr = cursor if cursor is Cursor else connect_as_user().cursor()
    if csr:
        query = f'SELECT ID FROM {currency_table_name} ' + condition
        csr.execute(query)
        rows = csr.fetchall()
        lst = [row[0] for row in rows]
        return lst
    else:
        raise Exception(get_currency_ids.__name__ + '  Cannot connect to the database to verify currency.')

def get_data_vendor_ids(cursor=None):
    csr = cursor if cursor is Cursor else connect_as_user().cursor()
    if csr:
        csr.execute(f"SELECT ID FROM {data_vendor_table_name}")
        rows = csr.fetchall()
        lst = [row[0] for row in rows]
        return lst
    else:
        raise Exception(get_data_vendor_ids.__name__ + '  Cannot connect to the database to verify data vendor.')

def fetch_portfolio(db_cursor = None, mongo_conn = None) -> Portfolio:
    cursor = db_cursor if isinstance(db_cursor, DictCursor) else connect_as_user().cursor(DictCursor)
    if cursor:
        query = f'SELECT * FROM {metadata_table_name}'
        cursor.execute(query)
        res = cursor.fetchall()
        portfolio = Portfolio()
        for row in res:
            field = row['field']
            if field == 'balance':
                portfolio.balance = float(row['val'])
            elif field == 'current_strategy':
                portfolio.current_strategy = str(row['val'])
        portfolio.lst_stocks = fetch_stocks(mongo_conn)
        return portfolio

def fetch_stocks(db_conn = None) -> list:
    conn = object()
    stock_collection_name = 'owned_stocks'
    if isinstance(db_conn, Collection):
        if db_conn.name == stock_collection_name:
            conn = db_conn
    else:
        # conn = get_mongo_db_conn(stock_collection_name)
        conn = get_remote_client(stock_collection_name)

    res = conn.find({})
    return [pickle.loads(doc['stock_bin']) for doc in res]

#ENDREGION

def fetch_info_of(symbol) -> str:
    ticker = yf.Ticker(symbol)
    try:
        return ticker.info
    except ValueError as e:
        raise Exception(e)


def fetch_label_latest_price(ticker) -> DataFrame:
    if isinstance(ticker, yf.Ticker):
        data = ticker.history(period='1d', interval='1m')
        data = label_OHLC_df(data, 2, small_change_threshold=0.004)
        data = get_OHLC_df(data.iloc[-1:]) # get only the last row
        return data
    else:
        raise TypeError('Ticker must be a valid instance of type yfinance.Ticker')