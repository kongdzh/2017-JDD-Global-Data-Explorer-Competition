# -*- coding: utf-8 -*

import pandas as pd
import numpy as np
from scipy.stats import mode
from datetime import datetime

login         = pd.read_csv("data/login_sort.csv")
login_id      = list(set(list(login['id'])))
group         = dict(list(login.groupby('id')))
trade         = pd.read_csv("data/trade.csv")

def feature_construct():
    trade = pd.read_csv("data/trade.csv")

    #给trade表打标签，若id在login表中，则打标签为1，否则为0
    trade['flag']         = map(lambda x:add_flag(x), trade['id'])

    #基本特征
    trade['is_scan'] = map(lambda x, y, z:is_scan(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    begin = datetime.now()
    trade['login_type'] = map(lambda x, y, z:login_type(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    print datetime.now() - begin
    trade['login_from'] = map(lambda x, y, z:login_from(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['login_suc'] = map(lambda x, y, z:is_log_suc(x, y, z), trade['id'], trade['time_sec'], trade['flag'])

    #计数特征
    trade['login_times_day'] = map(lambda x, y, z, a, b:login_times_day(x, y, z, a, b), trade['id'], trade['time_sec'], trade['month'], trade['day'], trade['flag'])
    trade['login_times_3'] = map(lambda x, y, z, a:login_times_3(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['login_times_30'] = map(lambda x, y, z, a:login_times_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['dev_cnt_day'] = map(lambda x, y, z, a: dev_cnt_day(x, y, z, a), trade['id'], trade['time_sec'], trade['days'],trade['flag'])
    trade['dev_cnt_3'] = map(lambda x, y, z, a: dev_cnt__3(x, y, z, a), trade['id'], trade['time_sec'], trade['days'],trade['flag'])
    trade['dev_cnt_30']  = map(lambda x, y, z, a:dev_cnt_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['log_from_cnt_day'] = map(lambda x, y, z, a:log_from_cnt_day(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['log_from_cnt_3'] = map(lambda x, y, z, a: log_from_cnt_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['log_from_cnt_30'] = map(lambda x, y, z, a: log_from_cnt_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['ip_cnt_day'] = map(lambda x, y, z, a: ip_cnt_day(x, y, z, a), trade['id'], trade['time_sec'], trade['days'],trade['flag'])
    trade['ip_cnt_3'] = map(lambda x, y, z, a: ip_cnt_3(x, y, z, a), trade['id'], trade['time_sec'], trade['days'],trade['flag'])
    trade['ip_cnt_30'] = map(lambda x, y, z, a:ip_cnt_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['city_cnt_day'] = map(lambda x, y, z, a: city_cnt_day(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['city_cnt_3'] = map(lambda x, y, z, a: city_cnt_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['city_cnt_30'] = map(lambda x, y, z, a:city_cnt_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['type_cnt_day'] = map(lambda x, y, z, a: type_cnt_day(x, y, z, a), trade['id'], trade['time_sec'], trade['days'],trade['flag'])
    trade['type_cnt_3'] = map(lambda x, y, z, a: type_cnt_3(x, y, z, a), trade['id'], trade['time_sec'], trade['days'],trade['flag'])
    trade['type_cnt_30'] = map(lambda x, y, z, a:type_cnt_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['trade_cnt_day'] = map(lambda x, y, z, a: trade_cnt_day(x, y, z, a), trade['id'], trade['time_sec'],trade['month'], trade['day'])
    trade['trade_cnt_3'] = map(lambda x, y, z: trade_cnt_3(x, y, z), trade['id'], trade['time_sec'],trade['days'])
    trade['trade_cnt_30'] = map(lambda x, y, z:trade_cnt_30(x, y, z), trade['id'], trade['time_sec'], trade['days'])
    trade['log_fail_cnt_day'] = map(lambda x, y, z, a, b: log_fail_cnt_day(x, y, z, a, b), trade['id'],trade['time_sec'], trade['month'], trade['day'], trade['flag'])
    trade['log_fail_cnt_3'] = map(lambda x, y, z, a: log_fail_cnt_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['log_fail_cnt_30'] = map(lambda x, y, z, a:log_fail_cnt_30days(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['log_suc_cnt_day'] = map(lambda x, y, z, a, b: log_suc_cnt_day(x, y, z, a, b), trade['id'], trade['time_sec'],trade['month'], trade['day'], trade['flag'])
    trade['log_suc_cnt_3'] = map(lambda x, y, z, a: log_suc_cnt_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['log_suc_cnt_30'] = map(lambda x, y, z, a: log_suc_cnt_30days(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['timelong_10_day'] = map(lambda x, y, z, a: timelong_10_day(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['timelong_10_3'] = map(lambda x, y, z, a: timelong_10_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['timelong_10_30'] = map(lambda x, y, z, a:timelong_10_30days(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['city_type_cnt_day'] = map(lambda x, y, z, a: city_type_cnt_day(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['city_type_cnt_3'] = map(lambda x, y, z, a: city_type_cnt_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['city_type_cnt_30'] = map(lambda x, y, z, a:city_type_cnt_30days(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['scan_cnt_day'] = map(lambda x, y, z, a: scan_cnt_day(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['scan_cnt_3'] = map(lambda x, y, z, a: scan_cnt_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['scan_cnt_30'] = map(lambda x, y, z, a:scan_cnt_30days(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['trade_cnt_hour'] = map(lambda x, y, z, a, b:trade_cnt_one_hour(x, y, z, a, b), trade['id'], trade['time_sec'], trade['month'], trade['day'], trade['hour'])

    #统计特征
    trade['timelong_median_dis'] = map(lambda x, y, z, a:timelong_median_dis(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['ave_last_three_timelong'] = map(lambda x, y, z:ave_last_three_timelong(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['std_last_three_timelong'] = map(lambda x, y, z:std_last_three_timelong(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['max_last_three_timelong'] = map(lambda x, y, z:max_last_three_timelong(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['dis_max_min_timelong'] = map(lambda x, y, z:dis_last_three_timelong(x, y, z), trade['id'], trade['time_sec'], trade['flag'])

    #比率特征
    trade['dev_radio_day'] = map(lambda x, y, z, a: dev_radio_day(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['dev_radio_3'] = map(lambda x, y, z, a: dev_radio_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['dev_radio_30'] = map(lambda x, y, z, a:dev_radio_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['log_radio_day'] = map(lambda x, y, z, a: log_radio_day(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['log_radio_3'] = map(lambda x, y, z, a: log_radio_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['log_radio_30'] = map(lambda x, y, z, a:log_radio_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['ip_radio_day'] = map(lambda x, y, z, a: ip_radio_day(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['ip_radio_3'] = map(lambda x, y, z, a: ip_radio_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['ip_radio_30'] = map(lambda x, y, z, a:ip_radio_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['city_radio_day'] = map(lambda x, y, z, a: city_radio_day(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['city_radio_3'] = map(lambda x, y, z, a: city_radio_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['city_radio_30'] = map(lambda x, y, z, a:city_radio_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['type_radio_day'] = map(lambda x, y, z, a: type_radio_day(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['type_radio_3'] = map(lambda x, y, z, a: type_radio_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['type_radio_30'] = map(lambda x, y, z, a:type_radio_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['time_10_radio_day'] = map(lambda x, y, z, a: times_10_radio_day(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['time_10_radio_3'] = map(lambda x, y, z, a: times_10_radio_3(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['time_10_radio_30'] = map(lambda x, y, z, a:times_10_radio_30(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])
    trade['scan_radio_day'] = map(lambda x, y, z, a: scans_day_radio(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['scan_radio_3'] = map(lambda x, y, z, a: scans_3_radio(x, y, z, a), trade['id'], trade['time_sec'],trade['days'], trade['flag'])
    trade['scan_radio_30'] = map(lambda x, y, z, a:scans_30_radio(x, y, z, a), trade['id'], trade['time_sec'], trade['days'], trade['flag'])

    #类别特征
    trade['is_last_one_city'] = map(lambda x, y, z:is_last_one_city(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_last_suc'] = map(lambda x, y, z:is_last_suc(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_same_dev_two'] = map(lambda x, y, z:is_same_dev_two(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_same_ip_two'] = map(lambda x, y, z:is_same_ip_two(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_same_type_two'] = map(lambda x, y, z:is_same_type_two(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_scan_last'] = map(lambda x, y, z:is_scan_last(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_common_dev'] = map(lambda x, y, z:is_common_dev(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_common_log_from'] = map(lambda x, y, z: is_common_log_from(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_common_ip'] = map(lambda x, y, z: is_common_ip(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_common_city'] = map(lambda x, y, z: is_common_city(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_common_type'] = map(lambda x, y, z: is_common_type(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_arise_dev'] = map(lambda x, y, z: is_arise_dev(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_arise_log_from'] = map(lambda x, y, z: is_arise_log_from(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_arise_ip'] = map(lambda x, y, z: is_arise_ip(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_arise_city'] = map(lambda x, y, z: is_arise_city(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_arise_type'] = map(lambda x, y, z: is_arise_type(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_10_times'] = map(lambda x, y, z:is_10_times(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_ip_risk_last'] = map(lambda x, y, z: is_ip_risk_last(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_ip_risk_last_two'] = map(lambda x, y, z: is_ip_risk_last_two(x, y, z), trade['id'], trade['time_sec'], trade['flag'])
    trade['is_first_trade'] = map(lambda x:is_first_trade(x), trade['id'])
    trade['is_5sec_last'] = map(lambda x:is_5sec_last(x), trade['id'])
    trade['is_5sec_last_two'] = map(lambda x:is_5sec_last_two(x), trade['id'])
    trade['is_arise_risk'] = map(lambda x, y:is_previous_risk(x, y), trade['id'], trade['time_sec'])

    trade = trade.drop(['day', 'year', 'min', 'sec', 'time_sec', 'days'], axis=1)
    trade.to_csv("data/id_feature.csv", index=False)

#给trade表打标签
def add_flag(id):
    if id in login_id:
        return 1
    else:
        return 0

#基本特征

#距离交易最近的一次登录是否采用扫码方式
def is_scan(id, time, flag):
    if flag==0:
        return 2

    df = group[id].loc[group[id]['time_sec']<=time]
    if df.shape[0]==0:
        return 2
    else:
        i = -1
        while i>=(-df.shape[0]):
            if df.iloc[i]['result']==1:
                break
            i = i - 1
        if i<(-df.shape[0]):
            return 2

        return df.iloc[i]['scan']

#最近一次登录类型
def login_type(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec']<=time]
    if df.shape[0]==0:
        return 0
    else:
        i = -1
        while i>=(-df.shape[0]):
            if df.iloc[i]['result']==1:
                break
            i = i - 1
        if i<(-df.shape[0]):
            return 0
        return df.iloc[-1]['type']

#最近一次登录来源
def login_from(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec']<=time]
    if df.shape[0]==0:
        return 0
    else:
        i = -1
        while i>=(-df.shape[0]):
            if df.iloc[i]['result']==1:
                break
            i = i - 1
        if i<(-df.shape[0]):
            return 0
        return df.iloc[-1]['log_from']

#最近一次是否登陆成功
def is_log_suc(id, time, flag):
    if flag==0:
        return 2

    df = group[id].loc[group[id]['time_sec']<=time]
    if df.shape[0]==0:
        return 2
    else:
        return df.iloc[-1]['result']

#计数特征

#今天是第几次登录
def login_times_day(id, time, month, day, flag):
    if flag==0:
        return 0

    df = group[id].loc[group[id]['time_sec']<=time]
    df = df.loc[df['month']==month]
    df = df.loc[df['day']==day]

    return df.shape[0]

#往前3天登陆次数
def login_times_3(id, time, days, flag):
    if flag==0:
        return 0

    df = group[id].loc[group[id]['time_sec']<=time]
    df = df.loc[df['days']<days & df['days']>=(days-3)]

    return df.shape[0]

#往前27~30天登陆次数
def login_times_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec']<=time]
    df = df.loc[df['days']<(days-3) & df['days']>=(days-30)]

    return df.shape[0]

#当天登录设备使用次数
def dev_cnt_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec']<=time]
    df = df[(df['days']==days)]

    if df.shape[0]==0:
        return 0

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    devs = list(df['device'])
    return devs.count(devs[i])

#往前3天登录设备次数
def dev_cnt__3(id, time, days, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days']>=(days-3)) & df['days']<days]

    if df.shape[0] == 0:
        return 0

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    devs = list(df['device'])
    return devs.count(devs[i])

#登录设备往前27~30天使用次数
def dev_cnt_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec']<=time]
    df = df[(df['days']<days-3) & (df['days']>=(days-30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    devs = list(df['device'])
    return devs.count(devs[i])

#登录来源当天使用次数
def log_from_cnt_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] == days)]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    log_f = list(df['log_from'])
    return log_f.count(log_f[i])

#登录来源往前3天使用次数
def log_from_cnt_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days']<days) & df['days']>=(days-3)]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    log_f = list(df['log_from'])
    return log_f.count(log_f[i])

#登录来源往前30天使用次数
def log_from_cnt_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    log_f = list(df['log_from'])
    return log_f.count(log_f[i])

#当天ip使用数量
def ip_cnt_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    ips = list(df['ip'])
    return ips.count(ips[i])

#往前三天ip使用数量
def ip_cnt_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    ips = list(df['ip'])
    return ips.count(ips[i])

#登录ip往前27~30天使用次数
def ip_cnt_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    ips = list(df['ip'])
    return ips.count(ips[i])

#登录城市当天使用数量
def city_cnt_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    city_t = list(df['city'])
    return city_t.count(city_t[i])

#登录城市往前3天使用数量
def city_cnt_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    city_t = list(df['city'])
    return city_t.count(city_t[i])

#登录城市往前30天使用次数
def city_cnt_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    city_t = list(df['city'])
    return city_t.count(city_t[i])

#登录类型当天使用次数
def type_cnt_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days']==days]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    type_t = list(df['type'])
    return type_t.count(type_t[i])

#登录类型往前3天使用次数
def type_cnt_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    type_t = list(df['type'])
    return type_t.count(type_t[i])

#登录类型往前30天使用次数
def type_cnt_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 0

    type_t = list(df['type'])
    return type_t.count(type_t[i])

#往前27~30天交易总次数
def trade_cnt_30(id, time, days):
    df = trade.loc[trade['id']==id]
    df = df.loc[df['time_sec']<=time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    return df.shape[0]

#往前3天交易次数
def trade_cnt_3(id, time, days):
    df = trade.loc[trade['id']==id]
    df = df.loc[df['time_sec']<=time]
    df = df[(df['days'] < days) & (df['days'] > (days-3))]

    return df.shape[0]

#当天是第几次交易
def trade_cnt_day(id, time, month, day):
    df = trade.loc[trade['id']==id]
    df = df.loc[df['time_sec']<=time]
    df = df.loc[df['month']==month]
    df = df.loc[df['day']==day]

    return df.shape[0]

#往前30天登录失败总次数
def log_fail_cnt_30days(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    if df.shape[0]==0:
        return 0
    else:
        res = list(df['result'])
        return len(res) - res.count(1)

def log_fail_cnt_3(id, time, days, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    if df.shape[0] == 0:
        return 0
    else:
        res = list(df['result'])
        return len(res) - res.count(1)

#当天登录失败了几次
def log_fail_cnt_day(id, time, month, day, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df.loc[df['month']==month]
    df = df.loc[df['day']==day]

    if df.shape[0]==0:
        return 0
    else:
        res = list(df['result'])
        return len(res) - res.count(1)

#往前30天登陆成功总次数
def log_suc_cnt_30days(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    if df.shape[0]==0:
        return 0
    else:
        res = list(df['result'])
        return res.count(1)

#往前3天登录成功次数
def log_suc_cnt_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    if df.shape[0]==0:
        return 0
    else:
        res = list(df['result'])
        return res.count(1)

#当天登录成功了几次
def log_suc_cnt_day(id, time, month, day, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df.loc[df['month'] == month]
    df = df.loc[df['day'] == day]

    if df.shape[0] == 0:
        return 0
    else:
        res = list(df['result'])
        return res.count(1)

#当天登录持续时间有多少是10的倍数
def timelong_10_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    if df.shape[0]==0:
        return 0

    return len([num for num in df['timelong'].values if num % 10 == 0])

#往前3天登录持续时间有多少是10的倍数
def timelong_10_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    if df.shape[0]==0:
        return 0

    return len([num for num in df['timelong'].values if num % 10 == 0])

#往前30天登录持续时间有多少是10的倍数
def timelong_10_30days(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    if df.shape[0]==0:
        return 0

    return len([num for num in df['timelong'].values if num % 10 == 0])

#当天登录城市类别个数
def city_type_cnt_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    if df.shape[0]==0:
        return 0

    cits = list(df['city'])
    return len(set(cits))

#往前3天登录城市类别个数
def city_type_cnt_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    if df.shape[0]==0:
        return 0

    cits = list(df['city'])
    return len(set(cits))

#往前30天登录城市类别个数
def city_type_cnt_30days(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    if df.shape[0]==0:
        return 0

    cits = list(df['city'])
    return len(set(cits))

#当天登录扫码次数
def scan_cnt_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    if df.shape[0]==0:
        return 0

    scans  = list(df['scan'])
    return scans.count(1)

#往前3天登录扫码个数
def scan_cnt_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days-3))]

    if df.shape[0]==0:
        return 0

    scans  = list(df['scan'])
    return scans.count(1)

#往前30天采用扫码登陆的次数
def scan_cnt_30days(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] <= days) & (df['days'] > (days - 30))]

    if df.shape[0]==0:
        return 0

    scans  = list(df['scan'])
    return scans.count(1)

#交易前一小时内的交易次数
def trade_cnt_one_hour(id, time, month, day, hour):
    df = trade.loc[trade['id']==id]
    df = df.loc[df['month']==month]
    df = df.loc[df['day']==day]
    df = df[(df['hour']<=hour) & (df['hour']>=(hour-1))]

    return df.shape[0]

#统计特征

#登录持续时间与登录持续时间中位数差的绝对值
def timelong_median_dis(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] <= days) & (df['days'] > (days - 30))]

    if df.shape[0]==0:
        return -1

    timlongs = list(df['timelong'])
    return abs(timlongs[-1] - np.median(timlongs))

#最后三次登录时间间隔的平均值
def ave_last_three_timelong(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]<4:
        return -1

    return np.mean([df.iloc[-1]['time_sec']-df.iloc[-2]['time_sec'], df.iloc[-2]['time_sec']-df.iloc[-3]['time_sec'],
                   df.iloc[-3]['time_sec']-df.iloc[-4]['time_sec']])

#最后三次登录时间间隔的标准差
def std_last_three_timelong(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]<4:
        return -1

    return np.std([df.iloc[-1]['time_sec']-df.iloc[-2]['time_sec'], df.iloc[-2]['time_sec']-df.iloc[-3]['time_sec'],
                   df.iloc[-3]['time_sec']-df.iloc[-4]['time_sec']])

#最后三次的最大登录持续时间
def max_last_three_timelong(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]<3:
        return -1

    return max([df.iloc[-1]['timelong'], df.iloc[-2]['timelong'], df.iloc[-3]['timelong']])

#最后三次的最大最小登录时间只差
def dis_last_three_timelong(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0] < 3:
        return -1

    return max([df.iloc[-1]['timelong'], df.iloc[-2]['timelong'], df.iloc[-3]['timelong']]) - max([df.iloc[-1]['timelong'], df.iloc[-2]['timelong'], df.iloc[-3]['timelong']])

#比率特征
#登录设备当天使用占比
def dev_radio_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    devs = list(df['device'])
    rad  = devs.count(devs[i])/((len(devs)+i+1) * 1.0)
    return rad

#登录设备往前3天使用占比
def dev_radio_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    devs = list(df['device'])
    rad  = devs.count(devs[i])/((len(devs)+i+1) * 1.0)
    return rad

#登录设备往前30天使用占比
def dev_radio_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    devs = list(df['device'])
    rad  = devs.count(devs[i])/((len(devs)+i+1) * 1.0)
    return rad

#登录来源当天使用占比
def log_radio_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    log_f = list(df['log_from'])
    rad   = log_f.count(log_f[i])/((len(log_f)+i+1) * 1.0)
    return rad

#登录来源往前3天使用占比
def log_radio_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    log_f = list(df['log_from'])
    rad   = log_f.count(log_f[i])/((len(log_f)+i+1) * 1.0)
    return rad

#登录来源往前30天使用占比
def log_radio_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    log_f = list(df['log_from'])
    rad   = log_f.count(log_f[i])/((len(log_f)+i+1) * 1.0)
    return rad

#登录ip当天使用占比
def ip_radio_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    ips = list(df['ip'])
    rad = ips.count(ips[i])/((len(ips)+i+1) * 1.0)
    return rad

#登录ip往前3天使用占比
def ip_radio_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] <= days) & (df['days'] > (days - 30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    ips = list(df['ip'])
    rad = ips.count(ips[i])/((len(ips)+i+1) * 1.0)
    return rad

#登录ip往前30天使用占比
def ip_radio_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] <= days) & (df['days'] > (days - 30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    ips = list(df['ip'])
    rad = ips.count(ips[i])/((len(ips)+i+1) * 1.0)
    return rad

#登录城市当天使用占比
def city_radio_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    city_t = list(df['city'])
    rad   = city_t.count(city_t[-1])/((len(city_t)+i+1) * 1.0)
    return rad

#登录城市往前3天使用占比
def city_radio_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days-3))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    city_t = list(df['city'])
    rad   = city_t.count(city_t[-1])/((len(city_t)+i+1) * 1.0)
    return rad

#登录城市往前三十天使用占比
def city_radio_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    city_t = list(df['city'])
    rad   = city_t.count(city_t[-1])/((len(city_t)+i+1) * 1.0)
    return rad

#登录类型在当天所占比
def type_radio_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    type_t = list(df['type'])
    rad   = type_t.count(type_t[i])/((len(type_t)+i+1) * 1.0)
    return rad

#登录类型在往前3天所占比例
def type_radio_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    type_t = list(df['type'])
    rad   = type_t.count(type_t[i])/((len(type_t)+i+1) * 1.0)
    return rad


#登录类型往前30天使用占比
def type_radio_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    type_t = list(df['type'])
    rad   = type_t.count(type_t[i])/((len(type_t)+i+1) * 1.0)
    return rad

#当天10倍登录持续时间所占比例
def times_10_radio_day(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] == days]

    if df.shape[0]==0:
        return -1

    return len([num for num in df['timelong'].values if num % 10 == 0]) / len(list(df['timelong'])) * 1.0

#往前3天登录持续时间占比
def times_10_radio_3(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    if df.shape[0]==0:
        return -1

    return len([num for num in df['timelong'].values if num % 10 == 0]) / len(list(df['timelong'])) * 1.0

#往前30天10倍登录时间所占比例
def times_10_radio_30(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    if df.shape[0]==0:
        return -1

    return len([num for num in df['timelong'].values if num % 10 == 0]) / len(list(df['timelong'])) * 1.0

#当天扫码登录所占比例
def scans_day_radio(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[df['days'] <= days]

    if df.shape[0] == 0:
        return -1

    scans = list(df['scan'])
    return scans.count(1) / len(scans) * 1.0

#往前3天扫码登录所占比例
def scans_3_radio(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < days) & (df['days'] >= (days - 3))]

    if df.shape[0] == 0:
        return -1

    scans = list(df['scan'])
    return scans.count(1) / len(scans) * 1.0

#往前30天采用扫码登录的比率
def scans_30_radio(id, time, days, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]
    df = df[(df['days'] < (days-3)) & (df['days'] >= (days - 30))]

    if df.shape[0] == 0:
        return -1

    scans = list(df['scan'])
    return scans.count(1) / len(scans) * 1.0

#类别特征

#上次登录是否是异地
def is_last_one_city(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec']<=time]

    if (df.shape[0]==0) or (df.shape[0]==1):
        return 2

    i = -1
    while i>(-df.shape[0]):
        if df.iloc[i]['result']==1:
            break
        i = i - 1

    if i==(-df.shape[0]):
        return 2

    if df.iloc[i]['city']==df.iloc[i-1]['city']:
        return 0
    else:
        return 1

#最近一次登录是否成功
def is_last_suc(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]==0:
        return 2

    return df.iloc[-1]['result']

#最后两次登陆的设备是否一样
def is_same_dev_two(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if (df.shape[0]==0) or (df.shape[0]==1):
        return 2

    i = -1
    while i > (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i == (-df.shape[0]):
        return 2

    if df.iloc[i]['device']==df.iloc[i-1]['device']:
        return 1
    else:
        return 0

#最后两次登陆的ip是否一样
def is_same_ip_two(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if (df.shape[0]==0) or (df.shape[0]==1):
        return 2

    i = -1
    while i > (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i == (-df.shape[0]):
        return 2

    if df.iloc[i]['ip']==df.iloc[i-1]['ip']:
        return 1
    else:
        return 0

#最后两次登录类型是否一致
def is_same_type_two(id, time, flag):
    if flag==0:
        return -1

    df = group[id].loc[group[id]['time_sec']<=time]

    if (df.shape[0]==0) or (df.shape[0]==1):
        return 2

    i = -1
    while i > (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i == (-df.shape[0]):
        return 2

    if df.iloc[i]['type']==df.iloc[i-1]['type']:
        return 1
    else:
        return 0

#是否采用扫码登录
def is_scan_last(id, time, flag):
   if flag==0:
       return -1
   df = group[id].loc[group[id]['time_sec']<=time]

   if df.shape[0]==0:
       return 2

   i = -1
   while i >= (-df.shape[0]):
       if df.iloc[i]['result'] == 1:
           break
       i = i - 1

   if i < (-df.shape[0]):
       return 2

   return df.iloc[i]['scan']

#登录设备是否是常用登录设备
def is_common_dev(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]==0:
        return 2

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    devs = list(df['devices'])[:i]

    if devs[-1]==int(mode(devs)[0]):
        return 1
    else:
        return 0

#是否是常用登录来源
def is_common_log_from(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]==0:
        return 2

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    logs = list(df['log_from'])[:i]

    if logs[-1]==int(mode(logs)[0]):
        return 1
    else:
        return 0

#是否是常用ip
def is_common_ip(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]==0:
        return 2

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    ips = list(df['ip'])[:i]

    if ips[-1]==int(mode(ips)[0]):
        return 1
    else:
        return 0

#是否是常用登录城市
def is_common_city(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]==0:
        return 2

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    citys = list(df['city'])[:i]

    if citys[-1]==int(mode(citys)[0]):
        return 1
    else:
        return 0

#是否是常用登录类型
def is_common_type(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]==0:
        return 2

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return 2

    types = list(df['type'])[:i]

    if types[-1]==int(mode(types)[0]):
        return 1
    else:
        return 0

#登录设备以前是否出现过
def is_arise_dev(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]==0:
        return 1

    i = -1
    while i > (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i == (-df.shape[0]):
        return 1

    if df.iloc[i]['dev'] in list(df['dev'])[:i]:
        return 1
    else:
        return 0

#登录来源以前是否出现过
def is_arise_log_from(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0] == 0:
        return 1

    i = -1
    while i > (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i == (-df.shape[0]):
        return 1

    if df.iloc[i]['log_from'] in list(df['log_from'])[:i]:
        return 1
    else:
        return 0

#登录ip以前是否出现过
def is_arise_ip(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0] == 0:
        return 1

    i = -1
    while i > (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i == (-df.shape[0]):
        return 1

    if df.iloc[i]['ip'] in list(df['ip'])[:i]:
        return 1
    else:
        return 0

#登录城市以前是否出现过
def is_arise_city(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0] == 0:
        return 1

    i = -1
    while i > (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i == (-df.shape[0]):
        return 1

    if df.iloc[i]['city'] in list(df['city'])[:i]:
        return 1
    else:
        return 0

#登录类型以前是否出现过
def is_arise_type(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0] == 0:
        return 1

    i = -1
    while i > (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i == (-df.shape[0]):
        return 1

    if df.iloc[i]['type'] in list(df['type'])[:i]:
        return 1
    else:
        return 0

#登录持续时间是否是10的倍数
def is_10_times(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0] == 0:
        return -1

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return -1

    if (df.iloc[-1]['timelong'] % 10)==0:
        return 1
    else:
        return 0

#登录ip是否出现过危险
def is_ip_risk_last(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0] == 0:
        return -1

    i = -1
    while i >= (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i < (-df.shape[0]):
        return -1

    ip = df.iloc[i]['ip']
    ti = df.iloc[i]['time_sec']
    df = df.loc[df['ip']==ip]
    df = df.loc[df['time_sec']<=ti]
    if 1 in list(df['is_risk']):
        return 1
    else:
        return 0

#上上次登录的ip是否出现过危险
def is_ip_risk_last_two(id, time, flag):
    if flag == 0:
        return -1

    df = group[id].loc[group[id]['time_sec'] <= time]

    if df.shape[0]==0 or df.shape[0]==1:
        return -1

    i = -1
    while i > (-df.shape[0]):
        if df.iloc[i]['result'] == 1:
            break
        i = i - 1

    if i == (-df.shape[0]):
        return -1

    ip = df.iloc[i-1]['ip']
    ti = df.iloc[i-1]['time_sec']
    df = df.loc[df['ip']==ip]
    df = df.loc[df['time_sec']<=ti]
    if 1 in list(df['is_risk']):
        return 1
    else:
        return 0

#是否是第一次交易
def is_first_trade(id):
    df = trade.loc[trade['id']==id]

    if df.shape[0]==1:
        return 1
    else:
        return 0

#与上一次交易是否发生在5秒内
def is_5sec_last(id):
    df = trade.loc[trade['id']==id]

    if df.shape[0]<2:
        return 0

    if (df.iloc[-1]['time_sec'] - df.iloc[-2]['time_sec']) <= 5:
        return 1
    else:
        return 0

#倒数第二次与倒数第三次交易是否发生在5秒内
def is_5sec_last_two(id):
    df = trade.loc[trade['id']==id]

    if df.shape[0]<3:
        return 0

    if (df.iloc[-2]['time_sec'] - df.iloc[-3]['time_sec']) <= 5:
        return 1
    else:
        return 0

#以前是否发生过危险交易
def is_previous_risk(id, time):
    df  = trade.loc[trade['id']==id]
    df  = df.loc[df['time_sec']<time]

    res = list(df['is_risk'])
    if 1 in res:
        return 1
    else:
        return 0

if __name__ == "__main__":
    feature_construct()
