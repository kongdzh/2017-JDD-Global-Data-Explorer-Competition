# -*- coding: utf-8 -*

import pandas as pd
import numpy as np
from sort_bytime import get_time

def trans_bool_(x):
    if x==False:
        return 0
    else:
        return 1

def dataProcess(name):
    s         = pd.read_csv(name)
    s['scan'] = map(lambda x:trans_bool_(x), s['is_scan'])
    s['secu']  = map(lambda x:trans_bool_(x), s['is_sec'])
    s = s.loc[s['timelong']>0].copy()
    s = s.loc[s['timelong']<300000].copy()
    return s.drop(['is_scan', 'is_sec'], axis=1)

if __name__ == "__main__":
    train_login = dataProcess("input/t_login.csv")
    test_login  = dataProcess("input/t_login_test.csv")
    train_login = get_time(train_login)
    test_login  = get_time(test_login)
    login       = pd.concat([train_login, test_login], axis=0)
    login.to_csv("data/login.csv", index=False)

    train_trade           = pd.read_csv("input/t_trade.csv")
    train_trade           = get_time(train_trade)
    train_trade           = train_trade.loc[train_trade['month']>1]
    test_trade            = pd.read_csv("input/t_trade_test.csv")
    test_trade['is_risk'] = list(np.ones(test_trade.shape[0])*(-1))
    test_trade            = get_time(test_trade)
    trade                 = pd.concat([train_trade, test_trade], axis=0)
    trade.to_csv("data/trade.csv", index=False)