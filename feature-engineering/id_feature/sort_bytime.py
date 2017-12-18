# -*- coding: utf-8 -*

import pandas as pd
import os

def cal_time(month, day, hour, min, sec):
    if month==1:
        days = day - 1
    elif month==2:
        days = 31 + day - 1
    elif month==3:
        days = 31 + 28 + day - 1
    elif month==4:
        days = 31 + 28 + 31 + day - 1
    elif month==5:
        days = 31 + 28 + 31 + 30 + day - 1
    else:
        days = 31 + 28 + 31 + 30 + 31 + day - 1

    return ((((days * 24) + hour) * 60) + min) * 60 + sec

def get_days(month, day):
    if month==1:
        days = day
    elif month==2:
        days = 31 + day
    elif month==3:
        days = 31 + 28 + day
    elif month==4:
        days = 31 + 28 + 31 + day
    elif month==5:
        days = 31 + 28 + 31 + 30 + day
    else:
        days = 31 + 28 + 31 + 30 + 31 + day

    return days

def get_time(s):
    s['day']       = s['time'].apply(lambda x: x.split()[0].split('-')[-1]).astype(int)
    s['month']     = s['time'].apply(lambda x: x.split()[0].split('-')[1]).astype(int)
    s['year']      = s['time'].apply(lambda x: x.split()[0].split('-')[0]).astype(int)
    s['hour']      = s['time'].apply(lambda x: x.split()[1].split(':')[0]).astype(int)
    s['min']       = s['time'].apply(lambda x: x.split()[1].split(':')[1]).astype(int)
    s['sec']       = s['time'].apply(lambda x: x.split()[1].split(':')[-1].split('.')[0]).astype(int)
    s['time_sec']  = map(lambda a, b, x, y, z:cal_time(a, b, x, y, z), s['month'], s['day'], s['hour'], s['min'], s['sec'])
    s['days']      = map(lambda x, y:get_days(x, y), s['month'], s['day'])

    return s

def sort_by_time(name):
    oname = name.replace('.csv', '_sort.csv')

    s = pd.read_csv(name)
    print("%s loaded" % name)
    s = get_time(s)
    s = s.sort_values(by='time_sec')
    s.to_csv(oname, index=False)
    print("sort %s done" % name)

if __name__ == "__main__":
    sort_by_time("data/login.csv")