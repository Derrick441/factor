import numpy as np
import pandas as pd
from data_pre import vpd_data_read
from operation_time import print_execute_time, date_addsub
from operation_database import data_to_mysql
from operation_rolling import pd_mean, pd_std
from datetime import datetime
np.seterr(divide='ignore', invalid='ignore')


# 数据处理
@print_execute_time
def spreadbias_datamanage(data_raw, t1):
    data = data_raw.copy()
    # 停牌:nan
    data.loc[data.volume == 0.0, ['close_adj', 'volume', 'changepct']] = np.nan
    # 股价
    data_price = data[['stock', 'date', 'close_adj']].pivot('date', 'stock', 'close_adj')
    data_price_ = data_price.ffill()  # 股价数据填充（后续需要）
    # return data_price_

    # 涨跌幅
    data_pct = data[['stock', 'date', 'changepct']].pivot('date', 'stock', 'changepct')

    # 全部日期
    date_list = data_pct.index
    # 需要计算的日期
    t = date_addsub(t1, (-3-1), 'date')  # 因子起始日t1+往前3个月+往前1个月
    date_list_ = date_list[date_list >= t]  # t日及之后的日期

    # 计算相关系数所需天数
    num = 240
    # 计算每一日期的价格偏离
    price_spread_list = []
    for i in date_list_:
        # t_temp = datetime.now()

        temp = data_pct[data_pct.index <= i]  # 当天及之前的所有涨跌幅数据
        temp_data = temp.iloc[-num:, :]  # 取num日的涨跌幅数据

        # temp_corr = temp_data.corr()  # nan值跳过，直接计算相关系数的pandas方法
        temp_data_ = temp_data.values
        temp_data_[np.isnan(temp_data_)] = 0.0  # 停牌涨跌幅数据设为0.0
        stock_na = (np.isnan(temp_data_).sum(axis=0) >= int(num/2))  # 超过一半时间都在停牌的股票
        temp_data_[:, stock_na] = np.nan  # 超过一半时间都在停牌的股票，涨跌幅数据设为nan
        temp_corr_ = np.corrcoef(temp_data_.T)  # 相关系数矩阵
        temp_corr = pd.DataFrame(temp_corr_)  # 转dataframe格式

        quantile = temp_corr.quantile(0.995, axis=0)  # 取每个股票的相关系数为99.5%分位的相似股票所对应的相关系数值
        stock_relative = ((temp_corr.values - quantile.values) >= 0.0) + 0.0  # 取相关系数值>=99.5%分位相关系数的股票

        stock_num = stock_relative.sum(axis=0)  # 每个股票的相似股票数（包括其本身）
        stock_num[stock_num == 0.0] = np.nan  # 相似股票数为0的股票为半数时间停牌的股票，设为nan

        temp_price = data_price_[data_price_.index == i]  # 当日股价
        temp_price_ = temp_price.fillna(0.0).values  # np.dot计算中nan需要变0.0
        reference_price = (np.dot(temp_price_, stock_relative) - temp_price_) / stock_num  # 参考价格（停牌股票数据重新变为nan)

        price_spread_ = np.log(temp_price.values) - np.log(reference_price)  # 计算股价与参考价格的价差
        price_spread_list.append(price_spread_)

        # print(i, datetime.now() - t_temp)
    # 数据合并
    price_spread_temp = np.concatenate(price_spread_list, axis=0)
    # 数据转dataframe格式
    price_spread = pd.DataFrame(price_spread_temp, index=date_list_, columns=data_price_.columns)
    return price_spread


# 核心因子计算
@print_execute_time
def spreadbias_keycompute(data, factor_name, factor_type, t1):
    # 核心计算
    num = 60
    # data_mean = data.rolling(num).mean()
    # data_std = data.rolling(num).std()
    data_mean = pd_mean(data, num)
    data_std = pd_std(data, num)
    spreadbias = (data - data_mean) / data_std
    factors = spreadbias.stack().reset_index().rename(columns={0: 'factor_value'})

    # 补充信息
    factors['factor_name'] = factor_name
    factors['factor_type'] = factor_type
    factors['compute_time'] = datetime.strftime(datetime.now(), '%Y%m%d %H:%M:%S')
    # 返回相关信息
    t_ = (datetime.strptime(t1, '%Y%m%d')).date()  # 因子起始日t1
    item_list = ['stock', 'date', 'factor_name', 'factor_type', 'factor_value', 'compute_time']
    result = factors.loc[factors.date >= t_, item_list]
    return result


# 因子计算
def spreadbias_compute(data_raw, t1, factor_name, factor_type):
    # 数据处理
    data = spreadbias_datamanage(data_raw, t1)
    # 核心因子计算
    factors = spreadbias_keycompute(data, factor_name, factor_type, t1)
    return factors


# 因子
@print_execute_time
def spreadbias_sql_compute_sql(factor_name, factor_type, t1, t2, save, factor_table):
    # 数据读取
    data_raw = vpd_data_read(t1, t2, 17)
    # 因子计算
    factors = spreadbias_compute(data_raw, t1, factor_name, factor_type)
    # 因子存储
    data_to_mysql(save, factors, factor_table, factor_name)
    return factors


if __name__ == '__main__':

    fac_sta_list = ['20100101', '20130101', '20160101', '20190101']
    fac_end_list = ['20121231', '20151231', '20181231', '20190601']
    time_list = zip(fac_sta_list, fac_end_list)
    for t1, t2 in time_list:
        print(t1, t2)

        spreadbias_sql_compute_sql('spreadbias', 'vpd', t1, t2, 1, 'fac_vpd_inv')

    # fac_test = spreadbias_sql_compute_sql('spreadbias', 'vpd', '20181228', '20181228', 0, 'test')
