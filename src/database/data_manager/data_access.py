try:
    import unzip_requirements
except ImportError as e:
    print(e)

from database.data_manager.init_queries import create_user_query, grant_all_query, create_database_query, setup_schema_queries, insert_metadata_query
from database.memsql.client import connect_as_root
from database.memsql.database import get_db_configs
import mysql.connector
from mysql.connector.cursor import CursorBase

#region SETUP
def setup_db(drop_old_info=False, db_cursor = None):
    cursor = db_cursor # if isinstance(db_cursor, CursorBase) else connect_as_root().cursor()
    create_db(cursor, drop_old_db=drop_old_info)
    create_SU_grant_all(cursor, drop_old_user=drop_old_info)

def setup_schema(cursor = None):
    csr = cursor # if isinstance(cursor, CursorBase) else connect_as_user().cursor()
    [csr.execute(query()) for query in setup_schema_queries]
    insert_default_values(csr)

def create_db(cursor, *, drop_old_db=False):
    if drop_old_db:
        cursor.execute('DROP DATABASE IF EXISTS algotrading')
    cursor.execute(create_database_query())

def create_SU_grant_all(cursor, *, drop_old_user=False):
    # username, password, host, port = get_db_configs('algotrader1') # only for local testing
    _, username, password, __, port = get_db_configs('remote_user')

    if drop_old_user:
        cursor.execute("DROP USER IF EXISTS '{}'@'{}'".format(username, '%'))

    cursor.execute(create_user_query('%', username, password))
    cursor.execute(grant_all_query(username, '%'))

def insert_default_values(cursor):
    cursor.execute('USE algotrading')
    query = insert_metadata_query('%s', '%s')
    init_values = (
        ('balance', 4000),
        ('current_strategy', 'SMACrossover')
    )
    cursor.executemany(query, init_values)

#endregion

def connect_as_user(uname='algotrader1', db_name='algotrading'):
    """
        autocommit is enabled by default
    """
    _, __, db_host = get_db_configs(section='database')
    username, password, _, port = get_db_configs(section=uname)
    conn = mysql.connector.connect(db=db_name, host=db_host, user=username,
                               passwd=password, port=int(port))
    conn._autocommit = True
    return conn

def connect_as_aws_root():
    username, password, db_host, port = get_db_configs(section='remote_root')
    return mysql.connector.connect(host=db_host, user=username,
                            passwd=password, port=int(port))

def connect_as_aws_user():
    db_name, username, password, _, port = get_db_configs(section='remote_user')
    print(username, password)
    _, __, db_host, ___ = get_db_configs(section='remote_root')
    return mysql.connector.connect(db=db_name, host=db_host, user=username,
                               passwd=password, port=int(port))
