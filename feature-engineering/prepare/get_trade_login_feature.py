# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import time
from datetime import datetime

begin = datetime.now()
## load trade

df_train_trade = pd.read_csv("data/t_trade_timestamp_split.csv")
df_test_trade = pd.read_csv("data/t_trade_test_timestamp_split.csv")

df_train_trade = df_train_trade.ix[df_train_trade["trade_month"].isin([2, 3, 4, 5]),:]
## load login
df_login_train = pd.read_csv("data/t_login_timesplit.csv")
df_login_test = pd.read_csv("data/t_login_test_timesplit.csv")
#
df_login = pd.concat([df_login_train, df_login_test], axis=0, ignore_index=True)

df_login["log_id"] = range(df_login.values.shape[0])


# train
log_id_list = []
df_train_trade_iter = df_train_trade.ix[:,["id","timestamp"]].values
for index,iter in enumerate(df_train_trade_iter):
    if index % 5000 == 0:
        print index,datetime.now() - begin
    df_login_filter = df_login.ix[(df_login["id"]==iter[0]) & (df_login["result"] == 1) &(df_login["timestamp"]<iter[1]),:]
    if df_login_filter.shape[0] == 0:
        log_id_list.append(-1)
    else:
        df_login_filter_max = df_login_filter.ix[df_login_filter["timestamp"].argmax(),:]
        log_id_list.append(df_login_filter_max["log_id"])
        

df_train_trade["log_id"] = log_id_list

# test
log_id_list = []
df_test_trade_iter = df_test_trade.ix[:,["id","timestamp"]].values
for index, iter in enumerate(df_test_trade_iter):
   if index % 5000 == 0:
      print index
   df_login_filter = df_login.ix[(df_login["id"]==iter[0]) & (df_login["result"] == 1) &(df_login["timestamp"]<iter[1]),:]
   if df_login_filter.shape[0] == 0:
      log_id_list.append(-1)
   else:
      df_login_filter_max = df_login_filter.ix[df_login_filter["timestamp"].argmax(),:]
      log_id_list.append(df_login_filter_max["log_id"])

df_test_trade["log_id"] = log_id_list

###############################################   
df_train_trade = df_train_trade.ix[:,["rowkey", "is_risk", "time_interval_log_trade"]]
df_test_trade = df_test_trade.ix[:,["rowkey", "is_risk", "time_interval_log_trade"]]
## 特征保存
df_train_trade.to_csv("data/train_trade_logid_5.csv",index=False)
df_test_trade.to_csv("data/test_trade_logid_5.csv",index=False)