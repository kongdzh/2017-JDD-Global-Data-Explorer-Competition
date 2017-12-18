# -*- coding: utf-8 -*
import pandas as pd

df    = pd.read_csv("data/id_feature.csv")

train = df.loc[df['month']<6]
train = train.drop(['rowkey', 'day', 'month', 'year', 'min', 'sec', 'time_sec', 'days'])
train.to_csv("data/id_train.csv", index=False)

test  = df.loc[df['month']==7]
test = test.drop(['rowkey', 'day', 'month', 'year', 'min', 'sec', 'time_sec', 'days', 'is_risk'])
test.to_csv("data/id_test.csv", index=False)