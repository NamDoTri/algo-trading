from backtesting import Strategy
import numpy as np

class BinaryClassificationStrategy(Strategy):
    price_delta = 0
    def decide_trade(self, prediction):
        # set take-profit and stop-loss prices
        close = self.data.Close
        upper, lower = close[-1] * (1 + np.r_[1, -1]*self.price_delta)

        # if prediction is up and not going long atm
        # do the opposite if the condition is reverse and is not going short
        if prediction == 1 and not self.position.is_long:
            self.buy(size=.2, tp = upper, sl = lower)
        elif prediction == -1 and not self.position.is_short:
            self.sell(size=.2, tp = lower, sl = upper)