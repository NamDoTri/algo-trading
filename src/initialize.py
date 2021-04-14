import time
from yfinance import Ticker
from MySQLdb.cursors import DictCursor
from business_logic.model_crud import load_saved_model_from_mongo
from business_logic.get_data import fetch_portfolio, fetch_label_latest_price
from database.data_manager.data_access import connect_as_user
from helpers.time import exchange_is_open


def main():
    cursor = connect_as_user().cursor(DictCursor)
    portfolio = fetch_portfolio(db_connection=cursor)
    lst_tickers = [Ticker(stock.symbol) for stock in portfolio.lst_stocks]

    decision_maker = object()
    if portfolio.is_using_ML():
        model_name = portfolio.current_strategy
        decision_maker = load_saved_model_from_mongo(model_name)
    else:
        # load TA based on enum
        pass

    while exchange_is_open():
        for ticker in lst_tickers:
            #   Fetch data
            data = fetch_label_latest_price(ticker)

            
            #   Make decision
            #   calculate portfolio, balance
        time.sleep(1) # refresh every second
    else:
        # tasks to run at the end of a trading day or when the market is not open
        # ingest log files
        # save transaction history
        # store market data for model training
        # retrain model
        pass

if __name__ == '__main__':
    main()