import sys, os
from datetime import datetime

def mute_log():
    sys.stdout = open(os.devnull, "w")

def unmute_log():
    file = sys.stdout
    file.close()
    sys.stdout = sys.__stdout__

def timestamp_log(msg):
    now = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
    print(now + ', ' + msg)