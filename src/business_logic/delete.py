from MySQLdb.cursors import Cursor
from database.data_manager.init_queries import delete_all_query, delete_query
from database.data_manager.init_queries import exchange_table_name, securities_table_name, daily_price_table_name, transaction_table_name
from .get_data import get_security_ids

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
            delete_transaction(cursor, lst_securityID=lst_IDs)

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

def delete_transaction(cursor, *, lst_securityID=None):
    """
        Give no ID to delete all entries
    """
    if not isinstance(cursor, Cursor):
        raise Exception("No database cursor found")

    elif lst_securityID == None:
        cursor.execute(delete_all_query(transaction_table_name))

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
                query = delete_query(transaction_table_name, condition)
                cursor.execute(query, (id,))