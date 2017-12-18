# -*- coding: utf-8 -*-

import pandas as pd

ip_days = pd.read_csv("data/train_ip_days_feature.csv")
ip_days = ip_days.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day', 'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'device', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

ip_id   = pd.read_csv("data/train_ip_id_feature.csv")
ip_id   = ip_id.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'deice', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

ip_dev   = pd.read_csv("data/train_ip_dev_feature.csv")
ip_dev   = ip_dev.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'deice', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

ip_log_from   = pd.read_csv("data/train_ip_log_from_feature.csv")
ip_log_from   = ip_log_from.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'deice', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

ip_login   = pd.read_csv("data/train_ip_login_feature.csv")
ip_login   = ip_login.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'deice', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

ip_result  = pd.read_csv("data/train_ip_result_feature.csv")
ip_result  = ip_result.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'deice', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

ip_type   = pd.read_csv("data/train_ip_type_feature.csv")
ip_type   = ip_type.drop(['rowkey', 'time', 'id', 'timestamp_x', 'trade_month', 'trade_day',  'is_risk', 'trade_hour',
                          'trade_min', 'log_id', 'timelong', 'deice', 'log_from', 'ip', 'city', 'result', 'timestamp', 'type'], axis=1)

ip_train = pd.concat([ip_days, ip_id, ip_dev, ip_log_from, ip_login, ip_result, ip_type], axis=1)

ip_train("data/ip_train.csv", index=False)