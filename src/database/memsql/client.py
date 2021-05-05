import mysql.connector
from .database import get_db_configs

def connect_as_root():
    username, password, host, port = get_db_configs('remote_root')
    conn = mysql.connector.connect(host=host, user=username, password=password, connect_timeout=5)
    if conn:
        print("Connected as root.")
        return conn
    else:
        raise Exception("Unable to connect to database")


if __name__ == "__main__":
    connect_as_root()
