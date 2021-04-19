from yfinance import Ticker
import math
from enums import Position
from business_logic.validation import ticker_is_valid

class Stock:
    symbol = ''
    buy_price = 0
    num_shares = 0
    bought_at = 0
    position = Position.IS_LONG

    def __init__(self, symbol, buy_price, num_shares, bought_at, position = Position.IS_LONG, is_testing = False):
        '''
            is_testing: ignore ticker validity
        '''
        ticker = Ticker(symbol)
        if not is_testing:
            if not ticker_is_valid(ticker):
                raise ValueError(f'Cannot find any stocks with symbol {symbol}.')
        self.symbol = symbol
        self.buy_price = buy_price
        self.num_shares = num_shares
        self.bought_at = bought_at
        self.position = position

    def total_worth(self):
        return self.buy_price * self.num_shares

    def set_stock_quantity(self, price_per_share, cash):
        if isinstance(price_per_share, float) and isinstance(cash, float):
            self.num_shares = math.floor(cash/price_per_share)

    def __str__(self) -> str:
        return ', '.join([self.symbol, str(self.buy_price), str(self.num_shares)])