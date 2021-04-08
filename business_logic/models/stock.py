class Stock:
    buy_price = 0
    num_shares = 0
    bought_at = 0

    def __init__(self, buy_price, num_shares, bought_at):
        self.buy_price = buy_price
        self.num_shares = num_shares
        self.bought_at = bought_at

    def total_worth(self):
        return self.buy_price * self.num_shares
