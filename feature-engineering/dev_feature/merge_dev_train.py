# -*- coding: utf-8 -*-

import pandas as pd

dev_days = pd.read_csv("data/train_dev_days_feature.csv")
dev_days = dev_days.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day', 'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'device', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

dev_id   = pd.read_csv("data/train_dev_id_feature.csv")
dev_id   = dev_id.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'device', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

dev_ip   = pd.read_csv("data/train_dev_ip_feature.csv")
dev_ip   = dev_ip.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'device', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

dev_log_from   = pd.read_csv("data/train_dev_log_from_feature.csv")
dev_log_from   = dev_log_from.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'device', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

dev_login   = pd.read_csv("data/train_dev_login_feature.csv")
dev_login   = dev_login.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'device', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

dev_result  = pd.read_csv("data/train_dev_result_feature.csv")
dev_result  = dev_result.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'device', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

dev_type   = pd.read_csv("data/train_dev_type_feature.csv")
dev_type   = dev_type.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'device', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

dev_train = pd.concat([dev_days, dev_id, dev_ip, dev_log_from, dev_login, dev_result, dev_type], axis=1)

dev_train("data/dev_train.csv", index=False)