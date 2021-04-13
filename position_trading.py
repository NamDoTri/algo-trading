import math
from datetime import datetime
import yfinance as yf
from MySQLdb.cursors import DictCursor
from business_logic.model_crud import load_saved_model_from_mongo
from business_logic.get_data import fetch_portfolio
from business_logic.decision_making.strategies.SMACrossoverClass import SMACrossover
from business_logic.models.stock import Stock
from database.data_manager.data_access import connect_as_user
from helpers.printing import timestamp_log
from enums import Action, TAStrategy

def main():
    cursor = connect_as_user().cursor(DictCursor)
    portfolio = fetch_portfolio(db_cursor=cursor)
    decision_maker = object()
    today_date = datetime.now().strftime('%d-%m-%Y')

    if portfolio.is_using_ML():
        model_name = portfolio.current_strategy
        decision_maker = load_saved_model_from_mongo(model_name)
    else:
        tech_indicator = portfolio.current_strategy
        if tech_indicator == TAStrategy.SMACrossover.name:
            decision_maker = SMACrossover()

    if len(portfolio.lst_symbols) > 0:
        # prepare data
        query = ' '.join(portfolio.lst_symbols)
        data = yf.download(query, period='1mo', interval='1d', group_by='tickers')

        # loop through ticker list
        for symbol in portfolio.lst_symbols:
            ticker_data = data[symbol]
            decision = decision_maker.should_buy(ticker_data)
            stock = object()

            if decision == Action.BUY:
                current_price = data.Close[-1]
                num_shares = math.floor(portfolio.max_per_stock / current_price)
                stock = Stock(symbol, current_price, num_shares, today_date)
                portfolio.add_stock(stock)
                
            elif decision == Action.SELL:
                stock = portfolio.sell_stock(symbol)

            elif decision == Action.HOLD:
                continue
            
            msg = ', '.join([portfolio.current_strategy, decision, str(stock)])
            timestamp_log(msg)

        portfolio.save_portfolio()

    else:
        print('No stocks in the current portfolio.')

if __name__ == '__main__':
    main()