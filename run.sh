#sh run.sh

#id feature
python "feature-engineering/id_feature/dataProcess.py"
python "feature-engineering/id_feature/sort_bytime.py"
python "feature-engineering/id_feature/feature_construct.py"

#prepare
python "feature-engineering/prepare/get_login_time.py"
python "feature-engineering/prepare/get_trade_login_feature.py"
python "feature-engineering/prepare/merge_trade_login_by_logid.py"
python "feature-enginerring/id_feature/split_id_train_test.py"

#dev feature
python "feature-engineering/dev_feature/get_dev_days_feature.py" "data/train_trade_connect_login.csv" "data/train_dev_days_feature.csv"
python "feature-engineering/dev_feature/get_dev_id_feature.py" "data/train_trade_connect_login.csv" "data/train_dev_id_feature.csv"
python "feature-engineering/dev_feature/get_dev_ip_feature.py" "data/train_trade_connect_login.csv" "data/train_dev_ip_feature.csv"
python "feature-engineering/dev_feature/get_dev_log_from_feature.py" "data/train_trade_connect_login.csv" "data/train_dev_log_from_feature.csv"
python "feature-engineering/dev_feature/get_dev_login_feature.py" "data/train_trade_connect_login.csv" "data/train_dev_login_feature.csv"
python "feature-engineering/dev_feature/get_dev_result_feature.py" "data/train_trade_connect_login.csv" "data/train_dev_result_feature.csv"
python "feature-engineering/dev_feature/get_dev_type_feature.py" "data/train_trade_connect_login.csv" "data/train_dev_type_feature.csv"
python "feature-enginerring/dev_feature/merge_dev_train.py"

python "feature-engineering/dev_feature/get_dev_days_feature.py" "data/test_trade_connect_login.csv" "data/test_dev_days_feature.csv"
python "feature-engineering/dev_feature/get_dev_id_feature.py" "data/test_trade_connect_login.csv" "data/test_dev_id_feature.csv"
python "feature-engineering/dev_feature/get_dev_ip_feature.py" "data/test_trade_connect_login.csv" "data/test_dev_ip_feature.csv"
python "feature-engineering/dev_feature/get_dev_log_from_feature.py" "data/test_trade_connect_login.csv" "data/test_dev_log_from_feature.csv"
python "feature-engineering/dev_feature/get_dev_login_feature.py" "data/test_trade_connect_login.csv" "data/test_dev_login_feature.csv"
python "feature-engineering/dev_feature/get_dev_result_feature.py" "data/test_trade_connect_login.csv" "data/test_dev_result_feature.csv"
python "feature-engineering/dev_feature/get_dev_type_feature.py" "data/test_trade_connect_login.csv" "data/test_dev_type_feature.csv"
python "feature-enginerring/dev_feature/merge_dev_test.py"

#ip feature
python "feature-engineering/ip_feature/get_ip_days_feature.py" "data/train_trade_connect_login.csv" "data/train_ip_days_feature.csv"
python "feature-engineering/ip_feature/get_ip_id_feature.py" "data/train_trade_connect_login.csv" "data/train_ip_id_feature.csv"
python "feature-engineering/ip_feature/get_ip_dev_feature.py" "data/train_trade_connect_login.csv" "data/train_ip_dev_feature.csv"
python "feature-engineering/ip_feature/get_ip_log_from_feature.py" "data/train_trade_connect_login.csv" "data/train_ip_log_from_feature.csv"
python "feature-engineering/ip_feature/get_ip_login_feature.py" "data/train_trade_connect_login.csv" "data/train_ip_login_feature.csv"
python "feature-engineering/ip_feature/get_ip_result_feature.py" "data/train_trade_connect_login.csv" "data/train_ip_result_feature.csv"
python "feature-engineering/ip_feature/get_ip_type_feature.py" "data/train_trade_connect_login.csv" "data/train_ip_type_feature.csv"
python "feature-enginerring/dev_feature/merge_ip_train.py"

python "feature-engineering/ip_feature/get_ip_days_feature.py" "data/test_trade_connect_login.csv" "data/test_ip_days_feature.csv"
python "feature-engineering/ip_feature/get_ip_id_feature.py" "data/test_trade_connect_login.csv" "data/test_ip_id_feature.csv"
python "feature-engineering/ip_feature/get_ip_dev_feature.py" "data/test_trade_connect_login.csv" "data/test_ip_ip_feature.csv"
python "feature-engineering/ip_feature/get_ip_log_from_feature.py" "data/test_trade_connect_login.csv" "data/test_ip_log_from_feature.csv"
python "feature-engineering/ip_feature/get_ip_login_feature.py" "data/test_trade_connect_login.csv" "data/test_ip_login_feature.csv"
python "feature-engineering/ip_feature/get_ip_result_feature.py" "data/test_trade_connect_login.csv" "data/test_ip_result_feature.csv"
python "feature-engineering/ip_feature/get_ip_type_feature.py" "data/test_trade_connect_login.csv" "data/test_ip_type_feature.csv"
python "feature-enginerring/dev_feature/merge_ip_test.py"