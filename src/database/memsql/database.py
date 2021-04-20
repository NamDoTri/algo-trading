from configparser import ConfigParser
from pathlib import Path
import os
import subprocess

def start_db():
    setup_env_variables()    
    subprocess.run('docker-compose up')

def setup_env_variables():
    license_key, root_pwd, _ = get_db_configs()
    os.environ['LICENSE_KEY'] = license_key
    os.environ['ROOT_PWD'] = root_pwd

def get_db_configs(section = 'database'):
    """
        section: enum(database, root, algotrader1)

        Return username, password, host, port
    """
    print('Accessing config file from {}'.format(os.getcwd()))
    path = os.path.join(os.path.dirname(__file__), '../../db_config.ini')
    config = ConfigParser()
    config.read(path)

    if len(config.sections()) > 0:
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

        elif section == 'remote_root':
            root_config = config['SINGLESTORE_ROOT']
            username = config['AWS_MYSQL']['USERNAME']
            password = root_config['PASSWORD']
            host = config['AWS_MYSQL']['HOST']
            port = root_config['PORT']
            return (username, password, host, port)     

        elif section == 'remote_user':
            user_config = config['algotrader2']
            db_name = user_config['DB_NAME']
            username = user_config['USERNAME']
            password = user_config['PASSWORD']
            host = user_config['HOST']
            port = user_config['PORT']
            return (db_name, username, password, host, port)
       

    else:
        file = Path(path)
        if file.is_file():
            raise Exception("Can't parse config file")
        else:
            raise Exception('Config file not found.')


if __name__ == "__main__":
    start_db()