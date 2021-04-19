import pandas as pd
import numpy as np
from sklearn.metrics import f1_score

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def get_OHLC_df(orig_data) -> pd.DataFrame:
    if isinstance(orig_data, pd.DataFrame):
        data = orig_data.copy()
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
        return data
    else:
        return None

def label_OHLC_df(orig_data, period, small_change_threshold=None) -> pd.DataFrame:
    """
        orig_data: OHLC data in a DataFrame
        period: period of time to compare changes. The a number of the first few rows will be clipped because their changes cannot be computed.
        small_change_threshold: pct of change to be considered impartial. Leave as None by Default to have a dataset for binary classification (only ups and downs)
    """
    validate_input_df(orig_data)
    if not isinstance(period, int):
        raise ValueError('Period must be an integer.')
    else:
        data = orig_data.copy()
        data.Close = data.Close.pct_change(period)

        #labeling
        mask_increase = data.Close > 0
        data.loc[mask_increase, 'Close'] = 1

        if isinstance(small_change_threshold, float) and small_change_threshold < 1:
            threshold = small_change_threshold
            mask_remain = data.Close.between(-threshold, threshold)
            data.loc[mask_remain, 'Close' ] = 0 # ignore too small changes

        mask_decrease = data.Close < 0
        data.loc[mask_decrease, 'Close' ] = -1

        # crop out NaN values in the first few rows
        data = data.iloc[period:]
        return data

def split_train_test(orig_data, split_date):
    """
        Split time-series dataset
    """
    validate_input_df(orig_data)
    if isinstance(split_date, np.datetime64):
        data = orig_data.copy()
        train = data[ data.index <  split_date]
        y_train = train['Close']
        X_train = train.drop('Close', axis=1)

        test = data[data.index >= split_date]
        y_test = test['Close']
        X_test = test.drop('Close', axis=1)
        return X_train, X_test, y_train, y_test 

def validate_input_df(data) -> bool:
    if not isinstance(data, pd.DataFrame):
        raise TypeError('orig_data must be of type pandas.DataFrame.')
    else:
        if not 'Close' in data.columns: 
            raise Exception('DataFrame must be OHLC data.')
        if not data.Close.dtype == np.float:
            raise TypeError('Values in column Close must be of type numpy.float')
    return True

def prepare_data_train_model(clf, orig_data, split):
    data = get_OHLC_df(orig_data)
    data = label_OHLC_df(data, 2, small_change_threshold=0.004)
    X_train, X_test, y_train, y_test = split_train_test(data, split)
    clf.fit(X_train, y_train)
    return f1_eval(clf, X_test, y_test)

def f1_eval(clf, X_test, y_test):
    y_pred = clf.predict(X_test)
    return f1_score(y_test, y_pred, average='macro')