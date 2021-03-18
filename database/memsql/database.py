from configparser import ConfigParser
from pathlib import Path
import os
import subprocess
import argparse

def start_db(config_path):
    file = Path(config_path)
    if file.is_file():
        setup_env_variables(config_path)    
        subprocess.run('docker-compose up')
    else:
        raise Exception("Config file not found")

def setup_env_variables(config_path):
    license_key, root_pwd, _ = get_db_configs(config_path)
    os.environ['LICENSE_KEY'] = license_key
    os.environ['ROOT_PWD'] = root_pwd

def get_db_configs(config_path = './db_config.ini', section = 'database'):
    """
    'section': enum(database, root, algotrader1)

    return username, password, host, port
    """
    config = ConfigParser()
    config.read(config_path)

    if section == 'database':
        license_key = config['SINGLESTORE']['LICENSE_KEY']
        root_pwd = config['SINGLESTORE']['ROOT_PWD']
        host = config['SINGLESTORE']['HOST']
        return (license_key, root_pwd, host)

    elif section == 'root':
        root_config = config['SINGLESTORE_ROOT']
        username = root_config['USERNAME']
        password = root_config['PASSWORD']
        host = root_config['HOST']
        port = root_config['PORT']
        return (username, password, host, port)
        
    elif  section == 'algotrader1':
        user_config = config['algotrader1']
        username = user_config['USERNAME']
        password = user_config['PASSWORD']
        host = user_config['HOST']
        port = user_config['PORT']
        return (username, password, host, port)



if __name__ == "__main__":
    # parse cmd args
    parser = argparse.ArgumentParser(description="Start up a SingleStore DB node")
    parser.add_argument('config_path', help='path to database config file')
    args = vars(parser.parse_args())
    file_path = args['config_path']
    start_db(file_path)