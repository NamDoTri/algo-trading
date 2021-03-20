from database.data_manager.init_queries import create_user_query, grant_all_query, create_database_query, setup_schema_queries, exchange_table_name, securities_table_name, daily_price_table_name, currency_table_name, city_table_name, country_table_name, data_vendor_table_name
from database.data_manager.init_queries import insert_exchange_query, insert_security_query, insert_daily_price_query, insert_data_vendor_query, insert_currency_query, insert_city_query, insert_country_query
from database.data_manager.init_queries import delete_all_query, delete_query
from database.memsql.client import connect_as_root
from database.memsql.database import get_db_configs
from MySQLdb.cursors import Cursor
import MySQLdb

#region SETUP
def setup_db(config_path, *,drop_old_info=False):
    cursor = connect_as_root(config_path).cursor()
    create_db(cursor, drop_old_db=drop_old_info)
    create_SU_grant_all(cursor, config_path, drop_old_user=drop_old_info)

def setup_schema(cursor):
    if isinstance(cursor, Cursor):
        [cursor.execute(query()) for query in setup_schema_queries]
    else: 
        raise Exception("No database cursor found.")

def create_db(cursor, *, drop_old_db=False):
    if drop_old_db:
        cursor.execute('DROP DATABASE IF EXISTS algotrading')
    cursor.execute(create_database_query())

def create_SU_grant_all(cursor, config_path, *, drop_old_user=False):
    username, password, host, port = get_db_configs(config_path, 'algotrader1')

    if drop_old_user:
        cursor.execute("DROP USER IF EXISTS '{}'@'{}'".format(username, host))

    cursor.execute(create_user_query(host, username, password))
    cursor.execute(grant_all_query(username, host))
#endregion

#region INSERT
def insert_exchange(cursor, abbrev, *, exchange_name="", cityID, lst_cities = list()):
    """
        lst_cities: list of all cities in the database.
                    If not provided, the function will query the list 
                    upon every insertation, which affects performance
    """
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")
    elif not isinstance(abbrev, str):
        raise Exception("Invalid exchange abbreviation.")
    elif not isinstance(exchange_name, str):
        raise Exception("Invalid exchange name")
    elif len(lst_cities) == 0:
        lst = get_city_ids()
        if not (cityID in lst):
            raise Exception("Invalid city ID")
        else:
            sanitized_exchange_name = exchange_name.replace("'", "`")
            cursor.execute(insert_exchange_query(abbrev, sanitized_exchange_name, cityID))

def insert_security(cursor, exchangeID, abbrev, *, security_name, company_name="", currencyID, lst_exchanges=list(), lst_currencies=list()):
    """
        lst_exchanges: list of all exchanges in the database.
                    If not provided, the function will query the list 
                    upon every insertation, which affects performance
        lst_currencies: list of all recorded currencies in the database.
                    If not provided, the function will query the list 
                    upon every insertation, which affects performance
    """
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")
    elif len(lst_exchanges) == 0:
        lst = get_exchange_ids()
        if not (exchangeID in lst):
            raise Exception("Invalid exchange")
    if not isinstance(abbrev, str):
        raise Exception("Invalid security abbreviation.")
    elif not isinstance(security_name, str):
        raise Exception("Invalid security name.")
    elif not isinstance(company_name, str):
        raise Exception("Invalid company name.")
    elif len(lst_currencies) == 0:
        lst = get_currency_ids()
        if not (currencyID in lst):
            raise Exception("Invalid currency")
        else: 
            sanitized_security_name = security_name.replace("'", "`")
            sanitized_company_name = company_name.replace("'", "`")
            sanitized_security_name = sanitized_security_name if sanitized_security_name != None else abbrev
            cursor.execute(insert_security_query(exchangeID, abbrev, sanitized_security_name, sanitized_company_name, currencyID))

def insert_daily_price(cursor, securityID, data_vendorID, open_price, close_price, adjusted_close_price, high_price, low_price, *, lst_securities=list(), lst_data_vendors=list()):
    """
        lst_securities: list of all securities in the database.
                    If not provided, the function will query the list 
                    upon every insertation, which affects performance
    """
    lst = lst_securities if len(lst_securities) != 0 else get_security_ids()
    if not (securityID in lst):
        raise Exception("Invalid security")

    lst = lst_data_vendors if len(lst_data_vendors) != 0 else get_data_vendor_ids()
    if not (data_vendorID in lst):
        raise Exception("Invalid data vendor")

    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")
    elif not (isinstance(open_price, float)):
        raise Exception("Invalid value for open price.")
    elif not (isinstance(close_price, float)):
        raise Exception("Invalid value for close price.") 
    elif not (isinstance(adjusted_close_price, float)):
        raise Exception("Invalid value for adjusted close price.")
    elif not (isinstance(high_price, float)):
        raise Exception("Invalid value for high price.")
    elif not (isinstance(low_price, float)):
        raise Exception("Invalid value for low price.")
    else:
        cursor.execute(insert_daily_price_query(securityID, data_vendorID, open_price, close_price, adjusted_close_price, high_price, low_price))
    
def insert_data_vendor(cursor, vendor_name, website_url):
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")
    elif not isinstance(vendor_name, str):
        raise Exception("Invalid vendor name")
    elif not isinstance(website_url, str):
        raise Exception("Invalid vendor website url")
    else:
        sanitized_vendor_name = vendor_name.replace("'", "`")
        sanitized_website_url = website_url.replace("'", "`")
        cursor.execute(insert_data_vendor_query(sanitized_vendor_name, sanitized_website_url))

def insert_currency(cursor, abbrev, currency_name):
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")
    elif not isinstance(abbrev, str):
        raise Exception("Invalid currency abbreviation.")
    elif not isinstance(currency_name, str):
        raise Exception("Invalid currency name")
    else:
        cursor.execute(insert_currency_query(abbrev, currency_name))

def insert_city(cursor, abbrev, city_name, countryID, *, lst_countries = list()):
    """
        lst_countries: list of all country IDs in the database. 
                        If not provided, the function will query the list 
                        upon every insertation, which affects performance
    """
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")
    elif not isinstance(abbrev, str):
        raise Exception("Invalid city abbreviation.")
    elif not isinstance(city_name, str):
        raise Exception("Invalid city name.")
    elif len(lst_countries) == 0:
        lst = get_country_ids()
        if (countryID in lst):
            sanitized_city_name = city_name.replace("'", "`")
            cursor.execute(insert_city_query(abbrev, sanitized_city_name, countryID))
        else:
            raise Exception("Invalid country ID")
    
def insert_country(cursor, abbrev, country_name):
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")
    elif not isinstance(abbrev, str):
        raise Exception("Invalid country abbreviation.")
    elif not isinstance(country_name, str):
        raise Exception("Invalid country name.")
    else:
        sanitized_country_name = country_name.replace("'", "`")
        cursor.execute(insert_country_query(abbrev, sanitized_country_name))

#endregion

#region DELETE
def delete_exchange(cursor, *, lst_IDs=None):
    """
        Cascade Delete operation
        Give no ID to delete all entries
    """
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")
    elif lst_IDs == None:
        cursor.execute(delete_all_query(exchange_table_name))
        cursor.execute(delete_all_query(securities_table_name))
        cursor.execute(delete_all_query(daily_price_table_name))
    else:
        if not isinstance(lst_IDs, list):
            raise Exception("Invalid list of exchange IDs")
        else:
            try:
                lst_IDs = [int(id) for id in lst_IDs]
            except ValueError:
                raise Exception("Invalid list of exchange IDs")

            condition = "ID = %s"

            for id in lst_IDs:
                query = delete_query(exchange_table_name, condition).strip()
                cursor.execute(query, (id, ))

            lst_securities = list()
            if len(lst_IDs) > 1:
                lst_securities = get_security_ids(condition="WHERE exchangeID IN (" + ','.join(str(id) for id in lst_IDs) + ')') 
            else:
                lst_securities = get_security_ids(condition=f"WHERE exchangeID = {lst_IDs[0]}") 

            delete_security(cursor, lst_IDs=lst_securities)

def delete_security(cursor, *, lst_IDs=None):
    """
        Cascade: Delete entries in securities and daily_price tables
        Give no ID to delete all entries
    """
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")

    elif lst_IDs == None:
        cursor.execute(delete_all_query(securities_table_name))
        cursor.execute(delete_all_query(daily_price_table_name))

    else:
        if not isinstance(lst_IDs, list):
            raise Exception("Invalid list of security IDs")
        else:
            try:
                lst_IDs = [int(id) for id in lst_IDs]
            except ValueError:
                raise Exception("Invalid list of exchange IDs")

            condition = "ID = %s"
            for id in lst_IDs:
                query = delete_query(securities_table_name, condition)
                cursor.execute(query, (id,))

            delete_daily_price(cursor, lst_securityID=lst_IDs)

def delete_daily_price(cursor, *, lst_securityID=None):
    """
        Give no ID to delete all entries
    """
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")

    elif lst_securityID == None:
        cursor.execute(delete_all_query(daily_price_table_name))

    else:
        if not isinstance(lst_securityID, list):
            raise Exception("Invalid list of security IDs")
        else:
            try:
                lst_securityID = [int(id) for id in lst_securityID]
            except ValueError:
                raise Exception("Invalid list of exchange IDs")

            condition = "securityID = %s"
            for id in lst_securityID:
                query = delete_query(daily_price_table_name, condition)
                cursor.execute(query, (id,))
#endregion

#region GET DATA
def get_country_ids():
    conn = connect_as_user()
    if conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT ID FROM {country_table_name}')
        rows = cursor.fetchall()
        lst_countries = [row[0] for row in rows]
        return lst_countries
    else:
        raise Exception(get_country_ids.__name__ + '  Cannot connect to the database to verify country name.') 

def get_city_ids():
    conn = connect_as_user()
    if conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT ID FROM {city_table_name}')
        rows = cursor.fetchall()
        lst_cities = [row[0] for row in rows]
        return lst_cities
    else:
        raise Exception(get_city_ids.__name__ + '  Cannot connect to the database to verify city name.') 

def get_exchange_ids():
    conn = connect_as_user()
    if conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT ID FROM {exchange_table_name}')
        rows = cursor.fetchall()
        lst_exchanges = [row[0] for row in rows]
        return lst_exchanges
    else:
        raise Exception(get_exchange_ids.__name__ + '  Cannot connect to the database to verify exchange.')

def get_security_ids(*, condition = ''):
    conn = connect_as_user()
    if conn:
        cursor = conn.cursor()
        query = f'SELECT ID FROM {securities_table_name} ' + condition
        cursor.execute(query)
        rows = cursor.fetchall()
        lst_security = [row[0] for row in rows]
        return lst_security
    else:
        raise Exception(get_security_ids.__name__ + '  Cannot connect to the database to verify security.')

def get_currency_ids():
    conn = connect_as_user()
    if conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT ID FROM {currency_table_name}')
        rows = cursor.fetchall()
        lst = [row[0] for row in rows]
        return lst
    else:
        raise Exception(get_currency_ids.__name__ + '  Cannot connect to the database to verify currency.')

def get_data_vendor_ids():
    conn = connect_as_user()
    if conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT ID FROM {data_vendor_table_name}")
        rows = cursor.fetchall()
        lst = [row[0] for row in rows]
        return lst
    else:
        raise Exception(get_data_vendor_ids.__name__ + '  Cannot connect to the database to verify data vendor.')
#endregion

def connect_as_user(uname='algotrader1', db_name='algotrading'):
    """
        autocommit is enabled by default
    """
    _, __, db_host = get_db_configs(section='database')
    username, password, _, port = get_db_configs(section=uname)
    conn = MySQLdb.connect(db=db_name, host=db_host, user=username,
                               passwd=password, port=int(port))
    conn.autocommit(True)
    return conn
