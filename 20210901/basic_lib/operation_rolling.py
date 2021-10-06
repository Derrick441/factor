import numpy as np
import pandas as pd
from operation_time import print_execute_time
from data_pre import vpd_data_read, factor_read_one


@print_execute_time
def pd_mean(data, num):
    result_list = []
    lens = len(data)
    for i in range(num, lens+1):
        data_temp = data.iloc[i-num:i, :].copy()  # 取需要的数据
        flag = (np.isnan(data_temp).sum() > num/2)
        flag_na = flag[flag > 0].index
        data_temp[flag_na] = np.nan  # 半数为空值，全部设为空值
        result_temp = data_temp.mean()  # 计算均值，pandas自动忽略空值
        result_list.append(result_temp)
    result = pd.concat(result_list, axis=1).T
    result.index = data.index[-(lens-num+1):]
    return result


@print_execute_time
def pd_std(data, num):
    result_list = []
    lens = len(data)
    for i in range(num, lens+1):
        data_temp = data.iloc[i-num:i, :].copy()  # 取需要的数据
        flag = (np.isnan(data_temp).sum() > num/2)
        flag_na = flag[flag > 0].index
        data_temp[flag_na] = np.nan  # 半数为空值，全部设为空值
        result_temp = data_temp.std()  # 计算均值，pandas自动忽略空值
        result_list.append(result_temp)
    result = pd.concat(result_list, axis=1).T
    result.index = data.index[-(lens-num+1):]
    return result


'''
def data_read(t1, t2, max_need, fac_table, fac_name):
    # 数据读取
    basic_data = vpd_data_read(t1, t2, max_need)
    factor = factor_read_one(fac_table, fac_name, t1, t2, max_need)
    return basic_data, factor


@print_execute_time
def data_manage(basic_data, factor):
    # 剔除停牌
    data_temp = basic_data.copy()
    data_temp.loc[data_temp.volume == 0.0, ['close_adj', 'volume', 'changepct']] = np.nan
    temp_data = data_temp[~np.isnan(data_temp.volume)].copy()
    # 合并（以剔除后的数据为基准）
    temp_data = pd.merge(temp_data, factor, how='left')
    # 数据展开
    factor_p = temp_data[['date', 'stock', 'factor_value']].pivot('date', 'stock', 'factor_value')
    return factor_p


def roll_mean_compute(factor_p, roll_num):
    return pd_mean(factor_p, roll_num)


@print_execute_time
def roll_mean_run(t1, t2, max_need, fac_table, fac_name, roll_num):
    basic_data, factor = data_read(t1, t2, max_need, fac_table, fac_name)
    factor_p = data_manage(basic_data, factor)
    factor = roll_mean_compute(factor_p, roll_num)
    return factor


if __name__ == '__main__':
    t1, t2, max_need = '20181228', '20181228', 7
    fac_table, fac_name, roll_num = 'fac_vpd_mom', 'newhigh120', 120
    newhighm120 = roll_mean_run(t1, t2, max_need, fac_table, fac_name, roll_num)
'''