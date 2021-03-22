
database_name = 'algotrading'
exchange_table_name = 'exchange'
securities_table_name = 'securities'
daily_price_table_name = 'daily_price'
data_vendor_table_name = 'data_vendor'
currency_table_name = 'currency'
city_table_name = 'city'
country_table_name = 'country'


#region DATABASE & USERS
def create_database_query():
    return f'CREATE DATABASE IF NOT EXISTS {database_name};'

def create_user_query(host, username, password):
    return f'''
        CREATE USER '{username}'@'{host}' IDENTIFIED BY '{password}';
    '''

def grant_all_query(username, host):
    return f"GRANT ALL PRIVILEGES ON algotrading.* TO '{username}'@'{host}';"

#endregion

#region CREATE TABLES 
def create_exchange_table():
    return f'''
        CREATE TABLE IF NOT EXISTS {exchange_table_name} 
        (
            id BIGINT AUTO_INCREMENT,
            abbrev VARCHAR(64),
            exchange_name VARCHAR(255) NOT NULL,
            cityID INT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),
                PRIMARY KEY (id)
        ) AUTO_INCREMENT = 1;
    '''

def create_securities_table():
    return f'''
        CREATE TABLE IF NOT EXISTS {securities_table_name}
        (
            id BIGINT AUTO_INCREMENT,
            exchangeID BIGINT NOT NULL,
            abbrev VARCHAR(32) NOT NULL, 
            security_name VARCHAR(255) NOT NULL,
            company_name VARCHAR(255) NOT NULL,
            currencyID INT NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),
                PRIMARY KEY (id)
        ) AUTO_INCREMENT = 1 ;
    '''

def create_daily_price_table():
    """
        Table securities must be created first
    """
    return f'''
        CREATE TABLE IF NOT EXISTS {daily_price_table_name} 
        (
            id INT NOT NULL AUTO_INCREMENT,
            securityID INT NOT NULL,
            data_vendorID INT NOT NULL,
            open_price DECIMAL(12, 6) NULL DEFAULT NULL,
            close_price DECIMAL(12, 6) NULL DEFAULT NULL,
            adjusted_close_price DECIMAL(12, 6) NULL DEFAULT NULL,
            high_price DECIMAL(12, 6) NULL DEFAULT NULL,
            low_price DECIMAL(12, 6) NULL DEFAULT NULL,
            volume BIGINT NULL DEFAULT 0,
            price_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (id)
        );
    '''

def create_data_vendor_table():
    return f'''
        CREATE TABLE IF NOT EXISTS {data_vendor_table_name}
        (
            id INT NOT NULL AUTO_INCREMENT,
            vendor_name VARCHAR(20) NOT NULL DEFAULT '',
            website_url VARCHAR(50) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),
                PRIMARY KEY (id)
        );
    '''

def create_currency_table():
    return f'''
        CREATE TABLE IF NOT EXISTS {currency_table_name} 
        (
            id INT NOT NULL AUTO_INCREMENT,
            abbrev VARCHAR(32) NULL,
            currency_name VARCHAR(55) NOT NULL, 
            last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (id)
        ) AUTO_INCREMENT=1;
    '''

def create_city_table():
    return f'''
        CREATE TABLE IF NOT EXISTS {city_table_name}
        (
            id INT NOT NULL AUTO_INCREMENT,
            abbrev VARCHAR(32) NULL,
            city_name VARCHAR(55) NOT NULL,
            countryID INT NOT NULL,
            last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (id)
        ) AUTO_INCREMENT=1;
    '''

def create_country_table():
    return f'''
    CREATE TABLE IF NOT EXISTS {country_table_name}
    (
        id INT NOT NULL AUTO_INCREMENT,
        abbrev VARCHAR(32) NULL,
        country_name VARCHAR(55) NOT NULL,
        last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (id)
    ) AUTO_INCREMENT=1;
    '''
#endregion

#region INSERT
def insert_exchange_query(abbrev, exchange_name, cityID):
    return f'''
        INSERT INTO {exchange_table_name} (abbrev, exchange_name, cityID)
        VALUES ('{abbrev}', '{exchange_name}', {cityID})
    '''

def insert_security_query(exchangeID, abbrev, security_name, company_name, currencyID):
    return f'''
        INSERT INTO {securities_table_name} (exchangeID, abbrev, security_name, company_name, currencyID)
        VALUES ({exchangeID}, '{abbrev}', '{security_name}', '{company_name}', {currencyID})
    '''

def insert_daily_price_query(securityID, data_vendorID, date, open_price, close_price, adjusted_close_price, high_price, low_price, volume):
    return f'''
        INSERT INTO {daily_price_table_name} (securityID, data_vendorID, price_date, open_price, close_price, adjusted_close_price, high_price, low_price, volume)
        VALUES ({securityID}, {data_vendorID}, {date}, {open_price}, {close_price}, {adjusted_close_price}, {high_price}, {low_price}, {volume})
    '''

def insert_data_vendor_query(vendor_name, website_url):
    return f'''
        INSERT INTO {data_vendor_table_name} (vendor_name, website_url)
        VALUES ('{vendor_name}', '{website_url}')
    '''

def insert_currency_query(abbrev, currency_name):
    return f'''
        INSERT INTO {currency_table_name} (abbrev, currency_name) 
        VALUES ('{abbrev}', '{currency_name}')
    '''

def insert_city_query(abbrev, city_name, countryID):
    return f'''
        INSERT INTO {city_table_name} (abbrev, city_name, countryID) 
        VALUES ('{abbrev}', '{city_name}', {countryID}) 
    '''
    
def insert_country_query(abbrev, country_name):
    return f'''
        INSERT INTO {country_table_name} (abbrev, country_name)
        VALUES ('{abbrev}', '{country_name}')
    '''
#endregion

#region DELETE
def delete_query(table_name, condition):
    """
        condition: condition after the WHERE keyword
    """
    return f'''
        DELETE FROM {table_name} WHERE {condition}
    '''

def delete_all_query(table_name):
    return f'''
        TRUNCATE TABLE {table_name}
    '''
#endregion

setup_db_queries = [create_database_query, create_user_query, grant_all_query]
setup_schema_queries = [ create_exchange_table, create_securities_table, create_daily_price_table, 
                create_data_vendor_table, create_currency_table, create_city_table, create_country_table]
