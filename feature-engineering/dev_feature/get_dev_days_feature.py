# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import time
import sys
from datetime import datetime

begin = datetime.now()

## load login
df_login_train = pd.read_csv("data/t_login_timesplit.csv")
df_login_test = pd.read_csv("data/t_login_test_timesplit.csv")

df_login_train["month_day"] = df_login_train["login_month"].values*31 + df_login_train["login_day"].values
df_login_test["month_day"] = df_login_test["login_month"].values*31 + df_login_test["login_day"].values
#
raw_data_path = sys.argv[1]
df_trade_train = pd.read_csv(raw_data_path)
#
df_login = pd.concat([df_login_train, df_login_test], axis=0, ignore_index=True)


## train
## dev在交易前一段时间内出现的次数

his_10d_dev_appear_days_list = []

his_1month_dev_appear_days_list = []

his_2month_dev_appear_days_list = []
## dev_id
his_10d_dev_id_appear_days_list = []

his_1month_dev_id_appear_days_list = []

his_2month_dev_id_appear_days_list = []

df_trade_train_iter = df_trade_train.ix[:,["device","timestamp_x","id"]].values
df_trade_train = df_trade_train.ix[:,["rowkey","id","is_risk"]]

for index,iter in enumerate(df_trade_train_iter):
    if index % 5000 == 0:
        print index, datetime.now() - begin
    df_login_filter = df_login.ix[(df_login["device"]==iter[0]) & (df_login["timestamp"]<iter[1]),:]
    df_login_filter_2month = df_login_filter.ix[(df_login_filter["timestamp"]>=iter[1] - 1440*3600),:]
    df_login_filter_1month = df_login_filter_2month.ix[(df_login_filter_2month["timestamp"]>=iter[1] - 720*3600),:]
    df_login_filter_10d = df_login_filter_1month.ix[(df_login_filter_1month["timestamp"]>=iter[1] - 240*3600),:]

    if df_login_filter.shape[0] == 0:
         # dev
        his_10d_dev_appear_days_list.append(0)

        his_1month_dev_appear_days_list.append(0)

        his_2month_dev_appear_days_list.append(0)

        # dev_id
        his_10d_dev_id_appear_days_list.append(0)

        his_1month_dev_id_appear_days_list.append(0)

        his_2month_dev_id_appear_days_list.append(0)
        
    else:
        # dev
        his_10d_dev_appear_days_list.append(len(df_login_filter_2month["month_day"].unique()))

        his_1month_dev_appear_days_list.append(len(df_login_filter_1month["month_day"]))

        his_2month_dev_appear_days_list.append(len(df_login_filter_10d["month_day"].unique()))

        # dev_id
        his_10d_dev_id_appear_days_list.append(df_login_filter_2month.ix[df_login_filter_2month["id"]==iter[2],["month_day"]]["month_day"].unique().shape[0])

        his_1month_dev_id_appear_days_list.append(df_login_filter_1month.ix[df_login_filter_1month["id"]==iter[2],["month_day"]]["month_day"].unique().shape[0])

        his_2month_dev_id_appear_days_list.append(df_login_filter_10d.ix[df_login_filter_10d["id"]==iter[2],["month_day"]]["month_day"].unique().shape[0])

        
## type == 1
df_trade_train["his_10d_dev_appear_days"] = his_10d_dev_appear_days_list
            
df_trade_train["his_1month_dev_appear_days"] = his_1month_dev_appear_days_list

df_trade_train["his_2month_dev_appear_days"] = his_2month_dev_appear_days_list

df_trade_train["his_10d_dev_id_appear_days"] = his_10d_dev_id_appear_days_list

df_trade_train["his_1month_dev_id_appear_days"] = his_1month_dev_id_appear_days_list

df_trade_train["his_2month_dev_id_appear_days"] = his_2month_dev_id_appear_days_list

out_data_path = sys.argv[2]
df_trade_train.to_csv(out_data_path, index=False)