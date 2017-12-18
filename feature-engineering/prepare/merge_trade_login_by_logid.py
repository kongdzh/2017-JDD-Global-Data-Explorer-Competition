# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import time
from datetime import datetime
import matplotlib.pyplot as plt
import my_func

begin = datetime.now()
## load trade

df_train_trade = pd.read_csv("data/train_trade_logid_5.csv")
df_test_trade = pd.read_csv("data/test_trade_logid_5.csv")

## load login
df_login_train = pd.read_csv("data/t_login_timesplit.csv")
df_login_test = pd.read_csv("data/t_login_test_timesplit.csv")
#
df_login = pd.concat([df_login_train, df_login_test], axis=0, ignore_index=True)

df_login["log_id"] = range(df_login.values.shape[0])

## 关联logid
log_id_list = [iter for iter in df_train_trade["log_id"].values]
print "finish log_id_list", datetime.now() - begin
df_login_connect = df_login.ix[df_login["log_id"].isin(log_id_list),["log_id", "timelong", "device", "log_from", "ip", "city" ,"result", "timestamp", "type"]]
#df_login_connect.columns = ["log_id", "timelong", "device", "log_from", "ip", "city" ,"result", "login_timestamp", "type"]
df_train_trade = pd.merge(df_train_trade, df_login_connect, on="log_id", how="left", sort=False)
df_train_trade.ix[:,["rowkey","is_risk","id","timestamp","log_id", "timelong", "device", "log_from", "ip", "city" ,"result", "login_timestamp", "type"]]
df_train_trade.to_csv("feature/train_trade_connect_login.csv", index=False)
print "finish train", datetime.now() - begin
## 关联logid
log_id_list = [iter for iter in df_test_trade["log_id"].values]
print "finish log_id_list", datetime.now() - begin
df_login_connect = df_login.ix[df_login["log_id"].isin(log_id_list),["log_id", "timelong", "device", "log_from", "ip", "city" ,"result", "timestamp", "type"]]
#df_login_connect.columns = ["log_id", "timelong", "device", "log_from", "ip", "city" ,"result", "login_timestamp", "type"]
df_test_trade = pd.merge(df_test_trade, df_login_connect, on="log_id", how="left", sort=False)
df_test_trade.ix[:,["rowkey","is_risk","id","timestamp","log_id", "timelong", "device", "log_from", "ip", "city" ,"result", "login_timestamp", "type"]]
df_test_trade.to_csv("feature/test_trade_connect_login.csv", index=False)
print "finish train", datetime.now() - begin