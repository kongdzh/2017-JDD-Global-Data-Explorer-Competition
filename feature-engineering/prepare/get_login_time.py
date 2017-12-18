# -*- coding: utf-8 -*

import pandas as pd
import time

filename_list = ["input/t_login.csv", "input/t_login_test.csv"]
for index, filename in enumerate(filename_list):
    ## load data
    df_data = pd.read_csv(filename)
    df_data["login_month"] = df_data["time"].apply(lambda str_time : str_time.split(" ")[0].split("-")[1])
    df_data["login_day"] = df_data["time"].apply(lambda str_time : str_time.split(" ")[0].split("-")[2])
    df_data["login_hour"] = df_data["time"].apply(lambda str_time : str_time.split(" ")[1].split(":")[0])
    df_data["login_min"] = df_data["time"].apply(lambda str_time : str_time.split(" ")[1].split(":")[1])
    if index == 0:
        df_data.to_csv("data/t_login_timesplit.csv", index=False)
    else:
        df_data.to_csv("data/t_login_test_timesplit.csv", index=False)

df_train_trade = pd.read_csv("data/t_trade.csv")
df_test_trade = pd.read_csv("data/t_trade_test.csv")
mytime = df_train_trade["time"].apply(lambda str_time : time.strptime(str_time,"%Y-%m-%d %H:%M:%S")).reset_index()
mytime.columns = ["index","time"]
time_stamp = mytime["time"].apply(lambda tm:time.mktime(tm)).astype("int")
df_train_trade["timestamp"] = time_stamp.values

mytime = df_test_trade["time"].apply(lambda str_time : time.strptime(str_time,"%Y-%m-%d %H:%M:%S")).reset_index()
mytime.columns = ["index","time"]
time_stamp = mytime["time"].apply(lambda tm:time.mktime(tm)).astype("int")
df_test_trade["timestamp"] = time_stamp.values

df_train_trade.to_csv("data/t_trade_timestamp.csv", index=False)
df_test_trade.to_csv("data/t_trade_test_timestamp.csv", index=False)

filename_list = ["data/t_trade_timestamp.csv", "data/t_trade_test_timestamp.csv"]
for index, filename in enumerate(filename_list):
    ## load data
    df_data = pd.read_csv(filename)
    df_data["trade_month"] = df_data["time"].apply(lambda str_time : str_time.split(" ")[0].split("-")[1])
    df_data["trade_day"] = df_data["time"].apply(lambda str_time : str_time.split(" ")[0].split("-")[2])
    df_data["trade_hour"] = df_data["time"].apply(lambda str_time : str_time.split(" ")[1].split(":")[0])
    df_data["trade_min"] = df_data["time"].apply(lambda str_time : str_time.split(" ")[1].split(":")[1])
    if index == 0:
        df_data.to_csv("data/t_trade_timestamp_split.csv", index=False)
    else:
        df_data.to_csv("data/t_trade_test_timestamp_split.csv", index=False)