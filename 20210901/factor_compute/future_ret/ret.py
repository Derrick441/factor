import numpy as np
import pandas as pd
from operation_time import print_execute_time, date_addsub
from operation_database import data_from_mysql, data_to_mysql
from datetime import datetime


# 数据读取
def ret_data_read(t1, t2):
    # 起始时间调整
    t1_bf = date_addsub(t1, (-7), 'str')  # 数据前推7个月
    # sql
    sql = '''
    select 
    symbol as stock,date,close * adjustfactor as close_adj,volume,changepct
    from 
    data_day
    where 
    (date>=str_to_date({},'%Y%m%d')) and (date<=str_to_date({},'%Y%m%d'))
    '''.format(t1_bf, t2)
    # 读取
    result = data_from_mysql(sql)
    return result


# 数据处理
@print_execute_time
def ret_datamanage(data_raw, t1):
    data = data_raw.copy()
    # 停牌:nan
    data.loc[data.volume == 0.0, ['close_adj', 'volume', 'changepct']] = np.nan
    # 股价
    data_price = data[['stock', 'date', 'close_adj']].pivot('date', 'stock', 'close_adj')
    data_price_ = data_price.ffill()
    return data_price_


# 核心因子计算
@print_execute_time
def ret_keycompute(data, factor_name, factor_type, t1, t2, p):
    # 核心计算
    data_pct = data.pct_change(p)
    ret = data_pct.shift(-p)
    if t1 == t2:
        ret_ = ret.dropna(how='all').tail(1)
    else:
        ret_ = ret
    factors = ret_.stack().reset_index().rename(columns={0: 'factor_value'})
    # 补充信息
    factors['factor_name'] = factor_name
    factors['factor_type'] = factor_type
    factors['compute_time'] = datetime.strftime(datetime.now(), '%Y%m%d %H:%M:%S')

    # 返回相关信息
    item_list = ['stock', 'date', 'factor_name', 'factor_type', 'factor_value', 'compute_time']
    result = factors[item_list]
    return result


# 因子计算
def ret_compute(data_raw, t1, t2, factor_name, factor_type, p):
    # 数据处理
    data = ret_datamanage(data_raw, t1)
    # 核心因子计算
    factors = ret_keycompute(data, factor_name, factor_type, t1, t2, p)
    return factors


# 全部计算
@print_execute_time
def ret_sql_compute_sql(factor_type, t1, t2, save, factor_table):
    # 数据读取
    data_raw = ret_data_read(t1, t2)

    # 计算多个未来收益率
    factor_names = ['ret001', 'ret005', 'ret010', 'ret020', 'ret060', 'ret120']
    periods = [1, 5, 10, 20, 60, 120]
    items = zip(factor_names, periods)
    for factor_name, p in items:

        # 因子计算、存储
        try:
            factors = ret_compute(data_raw, t1, t2, factor_name, factor_type, p)
            data_to_mysql(save, factors, factor_table, factor_name)
        except Exception as e:
            print(factor_name + 'compute failed\nthe problem is:', e)


if __name__ == '__main__':
    # 时间设置
    ret_t1, ret_t2 = '20100101', '20190601'

    # 未来收益率计算
    print('未来收益率计算：', ret_t1, ret_t2)
    ret_sql_compute_sql('ret', ret_t1, ret_t2, 1, 'rets')

    # # 时间设置
    # ret_t1, ret_t2 = '20181228', '20181228'
    #
    # # 未来收益率计算
    # print('未来收益率计算：', ret_t1, ret_t2)
    # ret_sql_compute_sql('ret', ret_t1, ret_t2, 1, 'rets')
