import sys, os

def mute_log():
    sys.stdout = open(os.devnull, "w")

def unmute_log():
    sys.stdout = sys.__stdout__