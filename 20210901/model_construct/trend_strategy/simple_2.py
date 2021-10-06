import numpy as np
import pandas as pd
from test_ic import ic_compute, ic_cheak
from test_group import group_compute_ret, group_rets_cheak


def fac_ic_cheak1(data_all, fac):
    item_list = ['stock', 'date', fac, 'ret001', 'ret005', 'ret010', 'ret020', 'ret060', 'ret120']
    data = data_all[item_list].set_index(['stock', 'date'])

    col_first, col_rest = data.columns[0], data.columns[1:]
    item_name = [col_first + '_' + x for x in col_rest]

    # ic计算
    ics = data.groupby(level=1).apply(ic_compute, 'spearman', item_name)
    # 查看ic情况
    fac_table, fac_name, ret_name = 'temp', fac, 'all'
    ic_cheak(ics, fac_table, fac_name, ret_name)


def fac_group_cheak1(data_all, fac, group_num):
    item_list = ['stock', 'date', fac, 'ret001', 'ret005', 'ret010', 'ret020', 'ret060', 'ret120']
    data = data_all[item_list].set_index(['stock', 'date'])

    col_first, col_rest = data.columns[0], data.columns[1:]
    item_name = [col_first + '_' + x for x in col_rest]

    # group计算
    group_rets = data.groupby(level=1).apply(group_compute_ret, group_num, item_name)
    # 查看分组收益情况
    fac_table, fac_name, ret_name = 'temp', fac, 'all'
    rets_cumsum_last = group_rets_cheak(group_rets, item_name, fac_table, fac_name, group_num, ret_name)


if __name__ == '__main__':

    # data_all = pd.read_pickle('C:\\Users\\Administrator\\Desktop\\data_20170601_20180601.pkl')
    # fac = 'sp010'
    # fac_ic_cheak1(data_all, fac)
    # group_num = 10
    # fac_group_cheak1(data_all, fac, group_num)

    data_all = pd.read_pickle('C:\\Users\\Administrator\\Desktop\\data_20170101_20180601_.pkl')
    fac = 'nh240_005_mean'
    # 去除新股（3个月）
    temp_data_ = data_all[~np.isnan(data_all.ma060)].copy()
    temp_data_ = temp_data_[temp_data_.sp010 > 0]
    # fac_ic_cheak1(temp_data_, fac)
    group_num = 10
    fac_group_cheak1(temp_data_, fac, group_num)
