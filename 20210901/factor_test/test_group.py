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


# 分组收益
def group_compute_ret(data, group_num, names):
    temp = data.copy()
    temp['group'] = pd.cut(temp.iloc[:, 0].rank(), group_num, labels=range(group_num))
    result = temp.iloc[:, 1:].groupby(['group']).mean()
    result.columns = names  # 调整对象名称
    # 收益调整
    for item in names:
        result[item] = result[item] / float(item[-3:])
    return result.T


@print_execute_time
def group_rets_cheak(group_rets, item_name, fac_table, fac_name, group_num, ret_name):
    # 数据格式调整
    temp = group_rets.copy()
    temp.columns = ['group_'+str(i) for i in range(group_num)]
    temp.reset_index(inplace=True)

    file_dir = 'C:\\Users\\Administrator\\Desktop\\' + fac_table+'_'+fac_name+'_'+ret_name+'_group_rets.xlsx'
    with pd.ExcelWriter(file_dir) as writer:

        rets_list = []
        rets_cumsum_list = []
        rets_cumsum_last_list = []
        for item in item_name:
            temp_data = temp[temp['level_1'] == item]
            temp_data_cumsum = temp_data.iloc[:, 2:].cumsum()

            temp_data.to_excel(writer, sheet_name=item, index=False)
            temp_data_cumsum.to_excel(writer, sheet_name=item+'_cumsum', index=False)

            rets_list.append(temp_data)
            rets_cumsum_list.append(temp_data_cumsum)
            rets_cumsum_last_list.append(temp_data_cumsum.dropna(how='all').tail(1))

        rets_cumsum_last = pd.concat(rets_cumsum_last_list)
        rets_cumsum_last.to_excel(writer, sheet_name='rets_cumsum_last', index=False)

    return rets_cumsum_last


# 每日分组情况
def group_compute_day(data, group_num):
    temp = data.copy()
    temp['group'] = pd.cut(temp.iloc[:, 0].rank(), group_num, labels=range(group_num))
    return temp.reset_index()[['stock', 'group']]


# 换手率
def turnover_compute(data):
    temp_all = len(data)
    temp_0 = len(data[data == 0])
    return (temp_all - temp_0) / temp_all


@print_execute_time
def group_turnover_cheak(data, adjust_num, group_num):
    temp = data.values
    to_all_list = []
    to_group_list = []
    for i in range(len(temp)-adjust_num):
        temp_data = temp[i] - temp[i+adjust_num]
        to_all_list.append(turnover_compute(temp_data))

        temp_df = pd.DataFrame(data=[temp[i], temp_data], index=['day_0', 'day_1']).T
        to_group_list.append(temp_df.groupby(['day_0'])['day_1'].apply(turnover_compute))

    result = pd.concat(to_group_list, axis=1).T.reset_index(drop=True)
    result.columns = ['group_' + str(i) for i in range(group_num)]
    result['all'] = to_all_list

    file_dir = 'C:\\Users\\Administrator\\Desktop\\'+fac_table+'_'+fac_name+'_'+str(group_num)+'_group_turnover.xlsx'
    with pd.ExcelWriter(file_dir) as writer:
        result.to_excel(writer, sheet_name='group_turnover', index=False)

    return result.mean()


@print_execute_time
def group_run(t1, t2, group_num, adjust_num, fac_table, fac_name, ret_table, ret_name):
    # 判断是否多对对
    if fac_name == 'all':
        print('factors can not be all, break')
        return 0
    else:
        # 取数
        data_basic, data_fac, data_ret = data_read(t1, t2, fac_table, fac_name, ret_table, ret_name)
        # 数据整理
        data, item_name = data_manage(data_basic, data_fac, data_ret)

        # group计算
        group_rets = data.groupby(level=1).apply(group_compute_ret, group_num, item_name)
        # 查看分组收益情况
        rets_cumsum_last = group_rets_cheak(group_rets, item_name, fac_table, fac_name, group_num, ret_name)

        # 每日分组情况
        group_day = data.groupby(level=1).apply(group_compute_day, group_num)\
                                         .reset_index()\
                                         .pivot('date', 'stock', 'group')\
                                         .ffill()
        # 换手率计算
        turnover_mean = group_turnover_cheak(group_day, adjust_num, group_num)

        return rets_cumsum_last, turnover_mean


if __name__ == '__main__':

    t1, t2, group_num, adjust_num = '20180101', '20190601', 10, 5
    fac_table, fac_name = 'fac_vpd_inv', 'spreadbias'
    ret_table, ret_name = 'future_ret', 'all'
    rets_cumsum_last, turnover_mean = group_run(t1, t2, group_num, adjust_num, fac_table, fac_name, ret_table, ret_name)
