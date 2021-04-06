from typing import Collection
import pymongo
import os 
from configparser import ConfigParser

def get_mongo_db_conn() -> pymongo.database.Database:
    db_name, db_host, db_port, collection_name = get_db_configs()

    try:
        client = pymongo.MongoClient(db_host, db_port)
        db = client[db_name]
        conn = db[collection_name]
        return conn
    except pymongo.errors.ConnectionFailure:
        raise ConnectionError('Cannot connect to MongoDB server.')

def get_db_configs():
    print(f'Accessing config file from {os.getcwd()}')
    path = os.path.join(os.path.dirname(__file__), '../db_config.ini')
    config = ConfigParser()
    config.read(path)

    if len(config.sections()) > 0:
        db_name = config['MONGODB']['DATABASE_NAME']
        db_host = config['MONGODB']['DATABASE_HOST']
        db_port = int(config['MONGODB']['DATABASE_PORT'])
        collection_name = config['MONGODB']['COLLECTION_NAME']
        return (db_name, db_host, db_port, collection_name)

def reset_db(db_connection = None):
    conn = db_connection if db_connection is pymongo.database.Database else get_mongo_db_conn()
    conn.drop()