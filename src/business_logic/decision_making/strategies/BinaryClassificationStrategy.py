from backtesting import Strategy
import numpy as np
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from business_logic.decision_making.data_prepration import prepare_data_train_model

class BinaryClassificationStrategy(Strategy):
    price_delta = .004
    split_date = np.datetime64('2020-03-31')
    
    def prepare_model(self, clf):
        self.clf = clf
        f1 = prepare_data_train_model(clf, self.data.df, self.split_date)
        print('f1 score: {}'.format(f1))

    def decide_trade(self, prediction):
        # set take-profit and stop-loss prices
        close = self.data.Close
        upper, lower = close[-1] * (1 + np.r_[1, -1]*self.price_delta)

        # if prediction is up and not going long atm
        # do the opposite if the condition is reverse and is not going short
        if prediction == 1 and not self.position.is_long:
            self.buy(size=.2, tp = upper, sl = lower)
            return
        elif prediction == -1 and not self.position.is_short:
            self.sell(size=.2, tp = lower, sl = upper)
            return
