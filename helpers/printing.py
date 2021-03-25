import sys, os

def mute_log():
    sys.stdout = open(os.devnull, "w")

def unmute_log():
    file = sys.stdout
    file.close()
    sys.stdout = sys.__stdout__