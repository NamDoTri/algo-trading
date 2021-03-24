import os
from pathlib import Path
import MySQLdb
from .database import get_db_configs

def connect_as_root(config_path):
    if Path(config_path).is_file():
        username, password, host, port = get_db_configs(config_path, 'root')
        conn = MySQLdb.connect(host=host, user=username,
                               passwd=password, port=int(port))
        if conn:
            print("Connected as root.")
            return conn
        else:
            raise Exception("Unable to connect to database")
    else:
        raise Exception('Config file not found')


if __name__ == "__main__":
    path = os.path.join(os.path.dirname(__file__), '../../db_config.ini')
    connect_as_root(path)
