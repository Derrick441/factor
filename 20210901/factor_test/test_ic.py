import numpy as np
import numba as nb
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from data_pre import vpd_data_read, factor_read_one, factor_read
from operation_time import print_execute_time


@print_execute_time
def data_read(t1, t2, fac_table, fac_name, ret_table, ret_name):
    # basic数据
    data_basic = vpd_data_read(t1, t2, 0)
    # 因子数据
    if fac_name != 'all':
        data_fac = factor_read_one(fac_table, fac_name, t1, t2, 0)
    else:
        data_fac = factor_read(fac_table, t1, t2, 0)
    # 收益数据
    if ret_name != 'all':
        data_ret = factor_read_one(ret_table, ret_name, t1, t2, 0)
    else:
        data_ret = factor_read(ret_table, t1, t2, 0)
    # 返回信息
    return data_basic, data_fac, data_ret


@print_execute_time
def data_manage(data_basic, data_fac, data_ret):
    # 展开
    pivot_fac = data_fac.pivot(index=['stock', 'date'], columns='factor_name', values='factor_value').reset_index()
    pivot_ret = data_ret.pivot(index=['stock', 'date'], columns='factor_name', values='factor_value').reset_index()
    # 根据因子数量调整数据顺序
    if fac_name != 'all':
        data_temp = pd.merge(data_basic, pivot_fac, how='left')
        data_temp = pd.merge(data_temp, pivot_ret, how='left')
    else:
        data_temp = pd.merge(data_basic, pivot_ret, how='left')
        data_temp = pd.merge(data_temp, pivot_fac, how='left')
    # 停牌数据处理
    data_temp.loc[data_temp.volume == 0.0, ['close_adj', 'volume', 'changepct']] = np.nan  # 成交量为0.0判断为停牌
    data = data_temp[~np.isnan(data_temp.volume)].copy()  # 取非停牌数据
    # 数据调整
    data.drop(['close_adj', 'volume', 'changepct'], axis=1, inplace=True)
    data.set_index(['stock', 'date'], inplace=True)
    # 后续对象名称
    col_first = data.columns[0]
    col_rest = data.columns[1:]
    item_name = [col_first + '_' + x for x in col_rest]
    return data, item_name


def ic_compute(data, method_, names):
    corrs = data.corr(method=method_)  # 'pearson' or 'spearman'
    result = corrs.iloc[1:, 0]  # 返回第一列数据，去除第一个
    result.index = names  # 调整ic名称
    return result


@print_execute_time
def ic_cheak(ics, fac_table, fac_name, ret_name):
    # 累计ic
    ics_cumsum = ics.cumsum()
    if len(ics.mean()) == 1:
        ics_mean = pd.DataFrame(data=[ics.mean()], index=[ics.name])
    else:
        ics_mean = pd.DataFrame(data=ics.mean(), index=ics.columns).T

    # csv输出
    file_dir = 'C:\\Users\\Administrator\\Desktop\\'+fac_table+'_'+fac_name+'_'+ret_name+'_ic.xlsx'
    with pd.ExcelWriter(file_dir) as writer:
        ics_mean.to_excel(writer, sheet_name='ics_mean')
        ics.to_excel(writer, sheet_name='ics')
        ics_cumsum.to_excel(writer, sheet_name='ics_cumsum')


@print_execute_time
def ic_run(t1, t2, fac_table, fac_name, ret_table, ret_name):
    # 判断是否多对
    if (fac_name == 'all') and (ret_name == 'all'):
        print('factors and rets can not both be all, break')
        return 0
    else:
        # 取数
        data_basic, data_fac, data_ret = data_read(t1, t2, fac_table, fac_name, ret_table, ret_name)
        # 数据整理
        data, item_name = data_manage(data_basic, data_fac, data_ret)
        # ic计算
        ics = data.groupby(level=1).apply(ic_compute, 'spearman', item_name)
        # 查看ic情况
        ic_cheak(ics, fac_table, fac_name, ret_name)
        return ics


if __name__ == '__main__':

    t1, t2 = '20180101', '20190601'
    fac_table, fac_name = 'fac_vpd_inv', 'spreadbias'
    ret_table, ret_name = 'future_ret', 'all'
    ics = ic_run(t1, t2, fac_table, fac_name, ret_table, ret_name)
