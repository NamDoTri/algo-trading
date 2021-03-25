import yfinance as yf
from MySQLdb.cursors import Cursor
from database.data_manager.init_queries import exchange_table_name, securities_table_name, currency_table_name, city_table_name, country_table_name, data_vendor_table_name
from database.data_manager.data_access import connect_as_user

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

def fetch_info_of(symbol) -> str:
    ticker = yf.Ticker(symbol)
    try:
        return ticker.info
    except ValueError as e:
        raise Exception(e)