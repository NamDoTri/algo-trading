import datetime
from pytz import timezone
from .cf_parser import get_configs

def exchange_is_open() -> bool:
    # fmt = "%Y-%m-%d %H:%M:%S"
    tz = get_exchange_timezone()
    time_now = datetime.datetime.now(timezone(tz)).time()
    open_time, close_time = get_exchange_open_close_time()
    return (time_now > open_time and time_now < close_time)


def get_exchange_timezone() -> str:
    config = get_configs()
    try:
        tz = config['EXCHANGE']['NY_TIMEZONE']
        return str(tz)
    except:
        raise Exception('Cannot find timezone setting in config file.')

 
def get_exchange_open_close_time() -> datetime.time:
    config = get_configs()
    try:
        open_time = config['EXCHANGE']['OPEN_TIME']
        close_time = config['EXCHANGE']['CLOSE_TIME']
        open_time = datetime.time.fromisoformat(open_time)
        close_time = datetime.time.fromisoformat(close_time)
        return (open_time, close_time)
    except:
        raise Exception('Cannot find exchange opening and closing time in config file.')
