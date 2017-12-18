# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import time
import sys
from datetime import datetime

begin = datetime.now()

## load login
no_use_list = []
df_login_train = pd.read_csv("data/t_login_timesplit.csv")
df_login_test = pd.read_csv("data/t_login_test_timesplit.csv")
#
df_login = pd.concat([df_login_train, df_login_test], axis=0, ignore_index=True)

raw_data_path = sys.argv[1]
df_trade_train = pd.read_csv(raw_data_path)

## login_cnt

history_dev_login_cnt_list = []

his_5min_dev_login_cnt_list = []

his_30min_dev_login_cnt_list = []

his_1h_dev_login_cnt_list = []

his_3h_dev_login_cnt_list = []

his_6h_dev_login_cnt_list = []

his_1d_dev_login_cnt_list = []

his_3d_dev_login_cnt_list = []

his_10d_dev_login_cnt_list = []

his_1month_dev_login_cnt_list = []

df_trade_train_iter = df_trade_train.ix[:,["device","timestamp"]].values
df_trade_train = df_trade_train.ix[:,["rowkey","id","is_risk"]]
for index, iter in enumerate(df_trade_train_iter):
    if index % 5000 == 0:
        print index, datetime.now() - begin
    df_login_filter = df_login.ix[(df_login["device"] == iter[0]) & (df_login["timestamp"] < iter[1]), :]
    df_login_filter_1month = df_login_filter.ix[(df_login_filter["timestamp"] >= iter[1] - 720 * 3600), :]
    df_login_filter_10d = df_login_filter_1month.ix[(df_login_filter_1month["timestamp"] >= iter[1] - 240 * 3600), :]
    df_login_filter_3d = df_login_filter_10d.ix[(df_login_filter_10d["timestamp"] >= iter[1] - 72 * 3600), :]
    df_login_filter_1d = df_login_filter_3d.ix[(df_login_filter_3d["timestamp"] >= iter[1] - 24 * 3600), :]
    df_login_filter_6h = df_login_filter_1d.ix[(df_login_filter_1d["timestamp"] >= iter[1] - 6 * 3600), :]
    df_login_filter_3h = df_login_filter_6h.ix[(df_login_filter_6h["timestamp"] >= iter[1] - 3 * 3600), :]
    df_login_filter_1h = df_login_filter_3h.ix[(df_login_filter_3h["timestamp"] >= iter[1] - 3600), :]
    df_login_filter_30min = df_login_filter_1h.ix[(df_login_filter_1h["timestamp"] >= iter[1] - 30 * 60), :]
    df_login_filter_5min = df_login_filter_30min.ix[(df_login_filter_30min["timestamp"] >= iter[1] - 5 * 60), :]

    if df_login_filter.shape[0] == 0:
        # 登陆的次数
        history_dev_login_cnt_list.append(0)

        his_5min_dev_login_cnt_list.append(0)

        his_30min_dev_login_cnt_list.append(0)

        his_1h_dev_login_cnt_list.append(0)

        his_3h_dev_login_cnt_list.append(0)

        his_6h_dev_login_cnt_list.append(0)

        his_1d_dev_login_cnt_list.append(0)

        his_3d_dev_login_cnt_list.append(0)

        his_10d_dev_login_cnt_list.append(0)

        his_1month_dev_login_cnt_list.append(0)
    else:
        # 登陆次数
        history_dev_login_cnt_list.append(len(df_login_filter["id"]))

        his_5min_dev_login_cnt_list.append(len(df_login_filter_5min["id"]))

        his_30min_dev_login_cnt_list.append(len(df_login_filter_30min["id"]))

        his_1h_dev_login_cnt_list.append(len(df_login_filter_1h["id"]))

        his_3h_dev_login_cnt_list.append(len(df_login_filter_3h["id"]))

        his_6h_dev_login_cnt_list.append(len(df_login_filter_6h["id"]))

        his_1d_dev_login_cnt_list.append(len(df_login_filter_1d["id"]))

        his_3d_dev_login_cnt_list.append(len(df_login_filter_3d["id"]))

        his_10d_dev_login_cnt_list.append(len(df_login_filter_10d["id"]))

        his_1month_dev_login_cnt_list.append(len(df_login_filter_1month["id"]))

df_trade_train['history_login_dev_cnt'] = history_dev_login_cnt_list

df_trade_train['his_5min_login_dev_cnt'] = his_5min_dev_login_cnt_list

df_trade_train['his_30min_login_dev_cnt'] = his_30min_dev_login_cnt_list

df_trade_train['his_1h_login_dev_cnt'] = his_1h_dev_login_cnt_list

df_trade_train['his_3h_login_dev_cnt'] = his_3h_dev_login_cnt_list

df_trade_train['his_6h_login_dev_cnt'] = his_6h_dev_login_cnt_list

df_trade_train['his_1d_login_dev_cnt'] = his_1d_dev_login_cnt_list

df_trade_train['his_3d_login_dev_cnt'] = his_3d_dev_login_cnt_list

df_trade_train['his_10d_login_dev_cnt'] = his_10d_dev_login_cnt_list

df_trade_train['his_1month_login_dev_cnt'] = his_1month_dev_login_cnt_list

out_data_path = sys.argv[2]
df_trade_train.to_csv(out_data_path, index=False)