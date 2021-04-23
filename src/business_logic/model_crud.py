import pymongo
import pickle
import time
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(os.getcwd()), '..')))
from database.mongo_client import get_mongo_db_conn

def save_model_to_mongo(model, model_name, db_connection = None) -> str:
    '''
        Pickle object model and save to MongoDB if not exists.
        Return: Id of the newly saved model
    '''
    conn = db_connection if db_connection is pymongo.database.Database else get_mongo_db_conn()

    if not model_exists(model_name, db_connection=conn):
        pickled = pickle.dumps(model)
        new_obj = conn.insert_one({'model': pickled, 'model_name': model_name, 'created_at': time.time(), 'last_trained': time.time()})
        return new_obj.inserted_id
    else:
        raise Exception(f"A model with name {model_name} already exists.")


def load_saved_model_from_mongo(model_name, db_connection = None) -> object:
    '''
        Query model from MongoDB if exists, then parse it.
        Return: binary value from the query parsed into a Python object
    '''
    conn = db_connection if db_connection is pymongo.database.Database else get_mongo_db_conn()
    res = conn.find_one({'model_name': model_name})
    if res is None:
        raise Exception(f'No model with the name {model_name} can be found in the database.')
    else:
        model = pickle.loads(res['model'])
        return model


def model_exists(model_name, db_connection = None) -> bool:
    if not isinstance(model_name, str):
        raise TypeError('Model name must be a string.')
    
    conn = db_connection if isinstance(db_connection, pymongo.collection.Collection) else get_mongo_db_conn()
    res = conn.find_one({'model_name': model_name})
    return not res is None

    
