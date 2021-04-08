from yfinance import Ticker
from business_logic.validation import ticker_is_valid

class Stock:
    symbol = ''
    buy_price = 0
    num_shares = 0
    bought_at = 0

    def __init__(self, symbol, buy_price, num_shares, bought_at):
        ticker = Ticker(symbol)
        if ticker_is_valid(ticker):
            self.symbol = symbol
            self.buy_price = buy_price
            self.num_shares = num_shares
            self.bought_at = bought_at
        else:
            raise ValueError(f'Cannot find any stocks with symbol {symbol}.')

    def total_worth(self):
        return self.buy_price * self.num_shares
