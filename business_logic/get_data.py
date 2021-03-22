from database.data_manager.init_queries import exchange_table_name, securities_table_name, currency_table_name, city_table_name, country_table_name, data_vendor_table_name
from database.data_manager.data_access import connect_as_user

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