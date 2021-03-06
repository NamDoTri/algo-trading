from database.data_manager.init_queries import create_user_query, grant_all_query, create_database_query, setup_schema_queries
from database.memsql.client import connect_as_root
from database.memsql.database import get_db_configs
from MySQLdb.cursors import Cursor
import MySQLdb

#region SETUP
def setup_db(drop_old_info=False):
    cursor = connect_as_root().cursor()
    create_db(cursor, drop_old_db=drop_old_info)
    create_SU_grant_all(cursor, drop_old_user=drop_old_info)

def setup_schema(cursor):
    if isinstance(cursor, Cursor):
        [cursor.execute(query()) for query in setup_schema_queries]
    else: 
        raise Exception("No database cursor found.")

def create_db(cursor, *, drop_old_db=False):
    if drop_old_db:
        cursor.execute('DROP DATABASE IF EXISTS algotrading')
    cursor.execute(create_database_query())

def create_SU_grant_all(cursor, *, drop_old_user=False):
    username, password, host, port = get_db_configs('algotrader1')

    if drop_old_user:
        cursor.execute("DROP USER IF EXISTS '{}'@'{}'".format(username, host))

    cursor.execute(create_user_query(host, username, password))
    cursor.execute(grant_all_query(username, host))
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
