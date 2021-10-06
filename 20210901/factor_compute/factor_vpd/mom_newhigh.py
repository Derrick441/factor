import numpy as np
import pandas as pd
from data_pre import vpd_data_read
from operation_time import print_execute_time, date_addsub
from operation_database import data_to_mysql
from datetime import datetime


# 数据处理
@print_execute_time
def newhigh_datamanage(data_raw, t1):
    data = data_raw.copy()
    # 停牌:nan
    data.loc[data.volume == 0.0, ['close_adj', 'volume', 'changepct']] = np.nan
    # 股价
    data_price = data[['stock', 'date', 'close_adj']].pivot('date', 'stock', 'close_adj')
    data_price_ = data_price.ffill()
    return data_price_


# 核心因子计算
@print_execute_time
def newhigh_keycompute(data, factor_name, factor_type, t1):
    periods = [5, 10, 20, 60, 120, 240]
    factor_names = ['newhigh005', 'newhigh010', 'newhigh020', 'newhigh060', 'newhigh120', 'newhigh240']
    items = zip(periods, factor_names)
    factor_list = []
    for n, name in items:
        # 核心计算
        roll_max = data.rolling(n).max()
        newhigh = data / roll_max
        factors = newhigh.stack().reset_index().rename(columns={0: 'factor_value'})
        # 补充信息
        factors['factor_name'] = name
        factors['factor_type'] = factor_type
        factors['compute_time'] = datetime.strftime(datetime.now(), '%Y%m%d %H:%M:%S')
        factor_list.append(factors)
    factors_all = pd.concat(factor_list)

    # 返回相关信息
    t_ = (datetime.strptime(t1, '%Y%m%d')).date()  # 因子起始日t1
    item_list = ['stock', 'date', 'factor_name', 'factor_type', 'factor_value', 'compute_time']
    result = factors_all.loc[factors_all.date >= t_, item_list]
    return result


# 因子计算
def newhigh_compute(data_raw, t1, factor_name, factor_type):
    # 数据处理
    data = newhigh_datamanage(data_raw, t1)
    # 核心因子计算
    factors = newhigh_keycompute(data, factor_name, factor_type, t1)
    return factors


# 因子
@print_execute_time
def newhigh_sql_compute_sql(factor_name, factor_type, t1, t2, save, factor_table):
    # 数据读取
    data_raw = vpd_data_read(t1, t2, 13)
    # 因子计算
    factors = newhigh_compute(data_raw, t1, factor_name, factor_type)
    # 因子存储
    data_to_mysql(save, factors, factor_table, factor_name)
    return factors


if __name__ == '__main__':

    fac_sta_list = ['20100101', '20130101', '20160101', '20190101']
    fac_end_list = ['20121231', '20151231', '20181231', '20190601']
    time_list = zip(fac_sta_list, fac_end_list)
    for t1, t2 in time_list:
        print(t1, t2)
        newhigh_sql_compute_sql('newhigh', 'vpd', t1, t2, 1, 'fac_vpd_mom')

    # fac_test = newhigh_sql_compute_sql('newhigh', 'vpd', '20181228', '20181228', 0, 'test')
