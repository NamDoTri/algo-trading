from configparser import ConfigParser
from pathlib import Path
import os

def get_configs() -> dict:
    print('Accessing config file from {}'.format(os.getcwd()))
    path = os.path.join(os.path.dirname(__file__), '../db_config.ini')

    file = Path(path)
    if file.is_file():
        config = ConfigParser()
        config.read(path)

        if len(config.sections()) > 0:
            return config
        else:
            raise Exception('Config file seems to be empty.')
    else:
        raise FileNotFoundError("Cannot find config file in the project root directory.")

    