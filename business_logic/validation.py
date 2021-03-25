from database.data_manager.init_queries import exchange_table_name, currency_table_name, securities_table_name, data_vendor_table_name
from database.data_manager.data_access import connect_as_user

def exchange_exists(exchange_name='', exchange_abbrev='') -> bool:
    query = f"SELECT ID FROM {exchange_table_name} WHERE exchange_name = '{exchange_name}' OR abbrev = '{exchange_abbrev}'"
    return does_exist(query)

def currency_exists(currency_name='', currency_abbrev='') -> bool:
    query = f"SELECT ID FROM {currency_table_name} WHERE currency_name = '{currency_name}' OR abbrev = '{currency_abbrev}'"
    return does_exist(query)

def security_exists(security_name='', security_abbrev='') -> bool:
    query = ''
    if len(security_name) == 0 and len(security_abbrev) == 0:
        return False
    elif len(security_name) > 0:
        query = f"SELECT ID FROM {securities_table_name} WHERE security_name = '{security_name}'"
        return does_exist(query)
    elif len(security_abbrev) > 0:
        query = f"SELECT ID FROM {securities_table_name} WHERE abbrev = '{security_abbrev}'"
        return does_exist(query)
    else:
        query = f"SELECT ID FROM {securities_table_name} WHERE security_name = '{security_name}' AND abbrev = '{security_abbrev}'"
        return does_exist(query)
        

def data_vendor_exists(vendor_name='', website=''):
    query = f"SELECT ID FROM {data_vendor_table_name} WHERE vendor_name = '{vendor_name}' OR website_url = '{website}'"
    return does_exist(query)

def does_exist(query) -> bool:
    conn = connect_as_user()
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    does_exist = len(res) > 0
    return does_exist
