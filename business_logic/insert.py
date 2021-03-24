from MySQLdb.cursors import Cursor
from database.data_manager.init_queries import insert_exchange_query, insert_security_query, insert_daily_price_query, insert_data_vendor_query, insert_currency_query, insert_city_query, insert_country_query
from .get_data import get_city_ids, get_exchange_ids, get_currency_ids, get_security_ids, get_data_vendor_ids, get_country_ids
from .validation import data_vendor_exists, exchange_exists, security_exists, currency_exists


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
            if not exchange_exists(exchange_name=sanitized_exchange_name, exchange_abbrev=abbrev):
                query = insert_exchange_query(abbrev, sanitized_exchange_name, cityID)
                cursor.execute(query)

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
            if not security_exists(security_name=sanitized_security_name, security_abbrev=abbrev):
                query = insert_security_query(exchangeID, abbrev, sanitized_security_name, sanitized_company_name, currencyID)
                cursor.execute(query)

def insert_daily_price(cursor, securityID, data_vendorID, date, open_price, close_price, adjusted_close_price, high_price, low_price, volume, *, lst_securities=list(), lst_data_vendors=list()):
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
        cursor.execute(insert_daily_price_query(securityID, data_vendorID, date, open_price, close_price, adjusted_close_price, high_price, low_price, volume))
    
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
        if not data_vendor_exists(vendor_name=sanitized_vendor_name, website=sanitized_website_url):
            query = insert_data_vendor_query(sanitized_vendor_name, sanitized_website_url)
            cursor.execute(query)

def insert_currency(cursor, abbrev, currency_name):
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")
    elif not isinstance(abbrev, str):
        raise Exception("Invalid currency abbreviation.")
    elif not isinstance(currency_name, str):
        raise Exception("Invalid currency name")
    else:
        if not currency_exists(currency_name=currency_name, currency_abbrev=abbrev):
            query = insert_currency_query(abbrev, currency_name)
            cursor.execute(query)

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
