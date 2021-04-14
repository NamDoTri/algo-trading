from enum import Enum

class Action(Enum):
    BUY = 1
    HOLD = 2
    SELL = 3

class TAStrategy(Enum):
    SMACrossover = 1