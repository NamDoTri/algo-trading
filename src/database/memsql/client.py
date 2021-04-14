import MySQLdb
from .database import get_db_configs

def connect_as_root():
    username, password, host, port = get_db_configs('root')
    conn = MySQLdb.connect(host=host, user=username,
                            passwd=password, port=int(port))
    if conn:
        print("Connected as root.")
        return conn
    else:
        raise Exception("Unable to connect to database")


if __name__ == "__main__":
    connect_as_root()
