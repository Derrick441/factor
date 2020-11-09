import pandas as pd
import numpy as np
import time

data1 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\factor\\temp\\' + 'factor_hq_pvcorravg_2012.pkl')
data2 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\factor\\temp\\' + 'factor_hq_pvcorrstd_2012.pkl')

ret20 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'+'all_band_adjvwap_hh_price_label20.pkl')

ret20_reset = ret20.reset_index().rename(columns={0: 'ret'})
ret20_2012 = ret20_reset[(ret20_reset.trade_dt > '20111231') & (ret20_reset.trade_dt < '20130101')]

data = pd.merge(ret20_2012, data1, how='left')
data = pd.merge(data, data2, how='left')
data.set_index(['trade_dt', 's_info_windcode'], inplace=True)


def perf_ic(data, m):
    temp = data.copy()
    # IC
    if m == 'IC':
        result = temp.corr(method='pearson')
    # rank IC
    else:
        result = temp.corr(method='spearman')
    # 返回未来收益与因子的相关系数
    return result.iloc[0, 1:]


result = data.groupby(level=0)\
             .apply(perf_ic, 'IC')\
             .apply(pd.Series)\
             .reset_index()
result.to_csv('D:\\wuyq02\\develop\\python\\data\\factor\\temp\\' + 'factor_hq_smartm_2012_ic.csv')
