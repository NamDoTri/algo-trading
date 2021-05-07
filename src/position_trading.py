try:
    import unzip_requirements
except ImportError as e:
    print(e)

import math
from datetime import datetime
import yfinance as yf
from business_logic.model_crud import load_saved_model_from_mongo
from business_logic.get_data import fetch_portfolio
from business_logic.decision_making.strategies.SMACrossoverClass import SMACrossover
from business_logic.models.stock import Stock
from database.data_manager.data_access import connect_as_aws_user
from database.mongo_client import get_remote_client
from helpers.printing import timestamp_log
from enums import Action, TAStrategy

conn = connect_as_aws_user()
conn.autocommit = True
cursor = conn.cursor(dictionary=True)

models_conn = get_remote_client('models')

def main(event, context):
    # cursor = connect_as_user().cursor(DictCursor) # for local database when testing
    print('Loading portfolio ...')
    portfolio = fetch_portfolio(db_cursor=cursor)
    print('Portfolio loaded. \n')
    decision_maker = object()
    today_date = datetime.now().strftime('%d-%m-%Y')

    if portfolio.is_using_ML():
        print('\nLoading ML models...')
        model_name = portfolio.current_strategy
        decision_maker = load_saved_model_from_mongo(model_name, models_conn)
        print('ML models loaded.')
    else:
        print('\nLoading TA class...')
        tech_indicator = portfolio.current_strategy
        if tech_indicator == TAStrategy.SMACrossover.name:
            decision_maker = SMACrossover()
        print('TA class loaded.')
    # UNCOMMENT IF TESTING
    if len(portfolio.lst_stocks) <= 0:
        portfolio.add_stock(Stock('AAPL', 0, 0, None))
    #     portfolio.add_stock(Stock('MSFT', 0, 0, None))
    #     portfolio.add_stock(Stock('AMZN', 0, 0, None))
    #     portfolio.add_stock(Stock('FB', 0, 0, None))
    #     portfolio.add_stock(Stock('GOOGL', 0, 0, None))
    

    if len(portfolio.lst_symbols) > 0:
        message = ''

        # prepare data
        print('\nLoading market data from Yahoo Finance...')
        query = ' '.join(portfolio.lst_symbols)
        data = yf.download(query, period='1mo', interval='1d', group_by='tickers')
        print('Market data loaded.')

        lst_columns = ('Open', 'High', 'Low', 'Close')

        # loop through ticker list
        print('\nEvaluating price and making decisions: ')
        for symbol in portfolio.lst_symbols:
            ticker_data = data.loc[:, (symbol, lst_columns)]
            ticker_data.columns = ticker_data.columns.get_level_values(1)
            decision = decision_maker.should_buy(ticker_data)
            stock = object()

            if decision == Action.BUY:
                current_price = data.Close[-1]
                num_shares = math.floor(portfolio.max_per_stock / current_price)
                stock = Stock(symbol, current_price, num_shares, today_date)
                portfolio.add_stock(stock)
                
            elif decision == Action.SELL:
                stock = portfolio.drop_stock(symbol)

            elif decision == Action.HOLD:
                continue
            
            msg = ', '.join([portfolio.current_strategy, str(decision), str(stock)])
            timestamp_log(msg)
            message += msg 
            message += '\n'

        print('\nSaving portfolio...')
        portfolio.save_portfolio(db_cursor=cursor)
        print('Portfolio saved to database.')

        return message
    else:
        print('No stocks in the current portfolio.')

if __name__ == '__main__':
    main(None, None)