import pandas as pd
from business_logic.decision_making.data_prepration import validate_input_df
from enums import Action

class SMACrossover:
    def __init__(self) -> None:
        '''
            n2 should always be bigger than n1
        '''
        self.n1 = 10
        self.n2 = 15
        self.sma1 = []
        self.sma2 = []

    def should_buy(self, data) -> Action:
        '''
            data: OHLC DataFrame
        '''
        if validate_input_df(data):
            if data.shape[0] >= (self.n2 + 1):
                data1 = data.iloc[-(self.n1 + 1):] # get historical data in n2 period + 1 day
                data2 = data.iloc[-(self.n2 + 1):] # get historical data in n2 period + 1 day

                self.sma1 = SMA(data1.Close, self.n1)
                self.sma2 = SMA(data2.Close, self.n2)
                
                if crossover(self.sma1, self.sma2):
                    return Action.BUY
                elif crossover(self.sma2, self.sma1):
                    return Action.SELL
                else:
                    return Action.HOLD

    def __repr__(self):
        return str(self.__class__) + ' with n1 = ' + str(self.n1) + ' and n2 = ' + str(self.n2)


def SMA(values, n):
    return pd.Series(values).rolling(n).mean()

def crossover(sma1, sma2):
    if (sma1[-2] < sma2[-2] and sma1[-1] > sma2[-1]):
        return True
    else:
        return False
