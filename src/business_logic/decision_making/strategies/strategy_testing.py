import pandas as pd 
import math
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from business_logic.decision_making.data_prepration import prepare_data_train_model
from business_logic.models.portfolio import Portfolio
from business_logic.models.stock import Stock
from enums import Position

def test_strategy(clf, portfolio, symbol, test_data):
    if isinstance(test_data, pd.DataFrame):
        print('Initial balance: ', portfolio.balance)
        for index, row in test_data.iterrows():
            X = [row[['Open', 'High', 'Low', 'Volume']]]
            prediction = clf.predict(X)
            stock = portfolio.get_stock(symbol)
            if stock is None:
                current_price = row['Close']
                time = str(row.index[0])
                num_shares = math.floor(portfolio.max_per_stock/current_price)
                stock = None
                if prediction == 1:
                    stock = Stock(symbol, current_price, num_shares, time, Position.IS_LONG, is_testing = True)
                    portfolio.add_stock(stock)            
                elif prediction == -1:
                    stock = Stock(symbol, current_price, -num_shares, time, Position.IS_SHORT, is_testing = True)
                    portfolio.add_stock(stock)
                print(f"Pred: {prediction} -- Added stock: {str(stock)} -- Total balance: {portfolio.balance}")
            elif not stock is None:
                if (prediction == 1 and stock.position == Position.IS_SHORT) or (prediction == -1 and stock.position == Position.IS_LONG):
                    portfolio.drop_stock(stock.symbol)
                    print(f"Pred: {prediction} -- Dropped stock: {str(stock)} -- Total balance: {portfolio.balance}")