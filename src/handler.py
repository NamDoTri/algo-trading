from position_trading import main

def handler(event, context):
    res = main()
    return res