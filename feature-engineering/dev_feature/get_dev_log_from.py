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
#log_from=1

history_dev_log_from_1_cnt_list = []

his_5min_dev_log_from_1_cnt_list = []

his_30min_dev_log_from_1_cnt_list = []

his_1h_dev_log_from_1_cnt_list = []

his_3h_dev_log_from_1_cnt_list = []

his_6h_dev_log_from_1_cnt_list = []

his_1d_dev_log_from_1_cnt_list = []

his_3d_dev_log_from_1_cnt_list = []

his_10d_dev_log_from_1_cnt_list = []

his_1month_dev_log_from_1_cnt_list = []

#log_from=2

history_dev_log_from_2_cnt_list = []

his_5min_dev_log_from_2_cnt_list = []

his_30min_dev_log_from_2_cnt_list = []

his_1h_dev_log_from_2_cnt_list = []

his_3h_dev_log_from_2_cnt_list = []

his_6h_dev_log_from_2_cnt_list = []

his_1d_dev_log_from_2_cnt_list = []

his_3d_dev_log_from_2_cnt_list = []

his_10d_dev_log_from_2_cnt_list = []

his_1month_dev_log_from_2_cnt_list = []

#log_from=10

history_dev_log_from_10_cnt_list = []

his_5min_dev_log_from_10_cnt_list = []

his_30min_dev_log_from_10_cnt_list = []

his_1h_dev_log_from_10_cnt_list = []

his_3h_dev_log_from_10_cnt_list = []

his_6h_dev_log_from_10_cnt_list = []

his_1d_dev_log_from_10_cnt_list = []

his_3d_dev_log_from_10_cnt_list = []

his_10d_dev_log_from_10_cnt_list = []

his_1month_dev_log_from_10_cnt_list = []

#log_from=16

history_dev_log_from_16_cnt_list = []

his_5min_dev_log_from_16_cnt_list = []

his_30min_dev_log_from_16_cnt_list = []

his_1h_dev_log_from_16_cnt_list = []

his_3h_dev_log_from_16_cnt_list = []

his_6h_dev_log_from_16_cnt_list = []

his_1d_dev_log_from_16_cnt_list = []

his_3d_dev_log_from_16_cnt_list = []

his_10d_dev_log_from_16_cnt_list = []

his_1month_dev_log_from_16_cnt_list = []

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
        # log_from=1的次数
        history_dev_log_from_1_cnt_list.append(0)

        his_5min_dev_log_from_1_cnt_list.append(0)

        his_30min_dev_log_from_1_cnt_list.append(0)

        his_1h_dev_log_from_1_cnt_list.append(0)

        his_3h_dev_log_from_1_cnt_list.append(0)

        his_6h_dev_log_from_1_cnt_list.append(0)

        his_1d_dev_log_from_1_cnt_list.append(0)

        his_3d_dev_log_from_1_cnt_list.append(0)

        his_10d_dev_log_from_1_cnt_list.append(0)

        his_1month_dev_log_from_1_cnt_list.append(0)
        #log_from=2
        history_dev_log_from_2_cnt_list.append(0)

        his_5min_dev_log_from_2_cnt_list.append(0)

        his_30min_dev_log_from_2_cnt_list.append(0)

        his_1h_dev_log_from_2_cnt_list.append(0)

        his_3h_dev_log_from_2_cnt_list.append(0)

        his_6h_dev_log_from_2_cnt_list.append(0)

        his_1d_dev_log_from_2_cnt_list.append(0)

        his_3d_dev_log_from_2_cnt_list.append(0)

        his_10d_dev_log_from_2_cnt_list.append(0)

        his_1month_dev_log_from_2_cnt_list.append(0)
        #log_from=10
        history_dev_log_from_10_cnt_list.append(0)

        his_5min_dev_log_from_10_cnt_list.append(0)

        his_30min_dev_log_from_10_cnt_list.append(0)

        his_1h_dev_log_from_10_cnt_list.append(0)

        his_3h_dev_log_from_10_cnt_list.append(0)

        his_6h_dev_log_from_10_cnt_list.append(0)

        his_1d_dev_log_from_10_cnt_list.append(0)

        his_3d_dev_log_from_10_cnt_list.append(0)

        his_10d_dev_log_from_10_cnt_list.append(0)

        his_1month_dev_log_from_10_cnt_list.append(0)
        #log_from=16
        history_dev_log_from_16_cnt_list.append(0)

        his_5min_dev_log_from_16_cnt_list.append(0)

        his_30min_dev_log_from_16_cnt_list.append(0)

        his_1h_dev_log_from_16_cnt_list.append(0)

        his_3h_dev_log_from_16_cnt_list.append(0)

        his_6h_dev_log_from_16_cnt_list.append(0)

        his_1d_dev_log_from_16_cnt_list.append(0)

        his_3d_dev_log_from_16_cnt_list.append(0)

        his_10d_dev_log_from_16_cnt_list.append(0)

        his_1month_dev_log_from_16_cnt_list.append(0)
    else:
        #log_from=1
        history_dev_log_from_1_cnt_list.append(df_login_filter[df_login_filter['log_from']==1].shape[0])

        his_5min_dev_log_from_1_cnt_list.append(df_login_filter_5min[df_login_filter_5min['log_from']==1].shape[0])

        his_30min_dev_log_from_1_cnt_list.append(df_login_filter_30min[df_login_filter_30min['log_from']==1].shape[0])

        his_1h_dev_log_from_1_cnt_list.append(df_login_filter_1h[df_login_filter_1h['log_from']==1].shape[0])

        his_3h_dev_log_from_1_cnt_list.append(df_login_filter_3h[df_login_filter_3h['log_from']==1].shape[0])

        his_6h_dev_log_from_1_cnt_list.append(df_login_filter_6h[df_login_filter_6h['log_from']==1].shape[0])

        his_1d_dev_log_from_1_cnt_list.append(df_login_filter_1d[df_login_filter_1d['log_from']==1].shape[0])

        his_3d_dev_log_from_1_cnt_list.append(df_login_filter_3d[df_login_filter_3d['log_from']==1].shape[0])

        his_10d_dev_log_from_1_cnt_list.append(df_login_filter_10d[df_login_filter_10d['log_from']==1].shape[0])

        his_1month_dev_log_from_1_cnt_list.append(df_login_filter_1month[df_login_filter_1month['log_from']==1].shape[0])
        #log_from=2
        history_dev_log_from_2_cnt_list.append(df_login_filter[df_login_filter['log_from']==2].shape[0])

        his_5min_dev_log_from_2_cnt_list.append(df_login_filter_5min[df_login_filter_5min['log_from']==2].shape[0])

        his_30min_dev_log_from_2_cnt_list.append(df_login_filter_30min[df_login_filter_30min['log_from']==2].shape[0])

        his_1h_dev_log_from_2_cnt_list.append(df_login_filter_1h[df_login_filter_1h['log_from']==2].shape[0])

        his_3h_dev_log_from_2_cnt_list.append(df_login_filter_3h[df_login_filter_3h['log_from']==2].shape[0])

        his_6h_dev_log_from_2_cnt_list.append(df_login_filter_6h[df_login_filter_6h['log_from']==2].shape[0])

        his_1d_dev_log_from_2_cnt_list.append(df_login_filter_1d[df_login_filter_1d['log_from']==2].shape[0])

        his_3d_dev_log_from_2_cnt_list.append(df_login_filter_3d[df_login_filter_3d['log_from']==2].shape[0])

        his_10d_dev_log_from_2_cnt_list.append(df_login_filter_10d[df_login_filter_10d['log_from']==2].shape[0])

        his_1month_dev_log_from_2_cnt_list.append(df_login_filter_1month[df_login_filter_1month['log_from']==2].shape[0])
        #log_from=10
        history_dev_log_from_10_cnt_list.append(df_login_filter[df_login_filter['log_from']==10].shape[0])

        his_5min_dev_log_from_10_cnt_list.append(df_login_filter_5min[df_login_filter_5min['log_from']==10].shape[0])

        his_30min_dev_log_from_10_cnt_list.append(df_login_filter_30min[df_login_filter_30min['log_from']==10].shape[0])

        his_1h_dev_log_from_10_cnt_list.append(df_login_filter_1h[df_login_filter_1h['log_from']==10].shape[0])

        his_3h_dev_log_from_10_cnt_list.append(df_login_filter_3h[df_login_filter_3h['log_from']==10].shape[0])

        his_6h_dev_log_from_10_cnt_list.append(df_login_filter_6h[df_login_filter_6h['log_from']==10].shape[0])

        his_1d_dev_log_from_10_cnt_list.append(df_login_filter_1d[df_login_filter_1d['log_from']==10].shape[0])

        his_3d_dev_log_from_10_cnt_list.append(df_login_filter_3d[df_login_filter_3d['log_from']==10].shape[0])

        his_10d_dev_log_from_10_cnt_list.append(df_login_filter_10d[df_login_filter_10d['log_from']==10].shape[0])

        his_1month_dev_log_from_10_cnt_list.append(df_login_filter_1month[df_login_filter_1month['log_from']==10].shape[0])
        #log_from=16
        history_dev_log_from_16_cnt_list.append(df_login_filter[df_login_filter['log_from']==16].shape[0])

        his_5min_dev_log_from_16_cnt_list.append(df_login_filter_5min[df_login_filter_5min['log_from']==16].shape[0])

        his_30min_dev_log_from_16_cnt_list.append(df_login_filter_30min[df_login_filter_30min['log_from']==16].shape[0])

        his_1h_dev_log_from_16_cnt_list.append(df_login_filter_1h[df_login_filter_1h['log_from']==16].shape[0])

        his_3h_dev_log_from_16_cnt_list.append(df_login_filter_3h[df_login_filter_3h['log_from']==16].shape[0])

        his_6h_dev_log_from_16_cnt_list.append(df_login_filter_6h[df_login_filter_6h['log_from']==16].shape[0])

        his_1d_dev_log_from_16_cnt_list.append(df_login_filter_1d[df_login_filter_1d['log_from']==16].shape[0])

        his_3d_dev_log_from_16_cnt_list.append(df_login_filter_3d[df_login_filter_3d['log_from']==16].shape[0])

        his_10d_dev_log_from_16_cnt_list.append(df_login_filter_10d[df_login_filter_10d['log_from']==16].shape[0])

        his_1month_dev_log_from_16_cnt_list.append(df_login_filter_1month[df_login_filter_1month['log_from']==16].shape[0])

df_trade_train['history_log_from_1_dev_cnt'] = history_dev_log_from_1_cnt_list

df_trade_train['his_5min_log_from_1_dev_cnt'] = his_5min_dev_log_from_1_cnt_list

df_trade_train['his_30min_log_from_1_dev_cnt'] = his_30min_dev_log_from_1_cnt_list

df_trade_train['his_1h_log_from_1_dev_cnt'] = his_1h_dev_log_from_1_cnt_list

df_trade_train['his_3h_log_from_1_dev_cnt'] = his_3h_dev_log_from_1_cnt_list

df_trade_train['his_6h_log_from_1_dev_cnt'] = his_6h_dev_log_from_1_cnt_list

df_trade_train['his_1d_log_from_1_dev_cnt'] = his_1d_dev_log_from_1_cnt_list

df_trade_train['his_3d_log_from_1_dev_cnt'] = his_3d_dev_log_from_1_cnt_list

df_trade_train['his_10d_log_from_1_dev_cnt'] = his_10d_dev_log_from_1_cnt_list

df_trade_train['his_1month_log_from_1_dev_cnt'] = his_1month_dev_log_from_1_cnt_list

df_trade_train['history_log_from_2_dev_cnt'] = history_dev_log_from_2_cnt_list

df_trade_train['his_5min_log_from_2_dev_cnt'] = his_5min_dev_log_from_2_cnt_list

df_trade_train['his_30min_log_from_2_dev_cnt'] = his_30min_dev_log_from_2_cnt_list

df_trade_train['his_1h_log_from_2_dev_cnt'] = his_1h_dev_log_from_2_cnt_list

df_trade_train['his_3h_log_from_2_dev_cnt'] = his_3h_dev_log_from_2_cnt_list

df_trade_train['his_6h_log_from_2_dev_cnt'] = his_6h_dev_log_from_2_cnt_list

df_trade_train['his_1d_log_from_2_dev_cnt'] = his_1d_dev_log_from_2_cnt_list

df_trade_train['his_3d_log_from_2_dev_cnt'] = his_3d_dev_log_from_2_cnt_list

df_trade_train['his_10d_log_from_2_dev_cnt'] = his_10d_dev_log_from_2_cnt_list

df_trade_train['his_1month_log_from_2_dev_cnt'] = his_1month_dev_log_from_2_cnt_list

df_trade_train['history_log_from_10_dev_cnt'] = history_dev_log_from_10_cnt_list

df_trade_train['his_5min_log_from_10_dev_cnt'] = his_5min_dev_log_from_10_cnt_list

df_trade_train['his_30min_log_from_10_dev_cnt'] = his_30min_dev_log_from_10_cnt_list

df_trade_train['his_1h_log_from_10_dev_cnt'] = his_1h_dev_log_from_10_cnt_list

df_trade_train['his_3h_log_from_10_dev_cnt'] = his_3h_dev_log_from_10_cnt_list

df_trade_train['his_6h_log_from_10_dev_cnt'] = his_6h_dev_log_from_10_cnt_list

df_trade_train['his_1d_log_from_10_dev_cnt'] = his_1d_dev_log_from_10_cnt_list

df_trade_train['his_3d_log_from_10_dev_cnt'] = his_3d_dev_log_from_10_cnt_list

df_trade_train['his_10d_log_from_10_dev_cnt'] = his_10d_dev_log_from_10_cnt_list

df_trade_train['his_1month_log_from_10_dev_cnt'] = his_1month_dev_log_from_10_cnt_list

df_trade_train['history_log_from_16_dev_cnt'] = history_dev_log_from_16_cnt_list

df_trade_train['his_5min_log_from_16_dev_cnt'] = his_5min_dev_log_from_16_cnt_list

df_trade_train['his_30min_log_from_16_dev_cnt'] = his_30min_dev_log_from_16_cnt_list

df_trade_train['his_1h_log_from_16_dev_cnt'] = his_1h_dev_log_from_16_cnt_list

df_trade_train['his_3h_log_from_16_dev_cnt'] = his_3h_dev_log_from_16_cnt_list

df_trade_train['his_6h_log_from_16_dev_cnt'] = his_6h_dev_log_from_16_cnt_list

df_trade_train['his_1d_log_from_16_dev_cnt'] = his_1d_dev_log_from_16_cnt_list

df_trade_train['his_3d_log_from_16_dev_cnt'] = his_3d_dev_log_from_16_cnt_list

df_trade_train['his_10d_log_from_16_dev_cnt'] = his_10d_dev_log_from_16_cnt_list

df_trade_train['his_1month_log_from_16_dev_cnt'] = his_1month_dev_log_from_16_cnt_list

out_data_path = sys.argv[2]
df_trade_train.to_csv(out_data_path, index=False)