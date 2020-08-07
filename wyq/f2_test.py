import pandas as pd
import numpy as np
import time
import statsmodels.api as sm

INDEX1 = 'zz500'
INDEX2 = 'all'
indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'

# 数据导入
mkt_indexprice = pd.read_pickle(indir+INDEX1+'/'+INDEX1+'_indexprice.pkl')
all_price = pd.read_pickle(indir+INDEX2+'/'+INDEX2+'_band_dates_stocks_closep.pkl')

# 数据处理
# mkt涨幅数据
mkt_rate = mkt_indexprice.reset_index()
mkt_rate['index_rate'] = mkt_rate.s_dq_change/mkt_rate.s_dq_preclose*100
mkt_rate['index_rate1'] = mkt_rate['index_rate'].shift(1)
mkt_rate['index_rate2'] = mkt_rate['index_rate'].shift(2)
mkt_rate['index_rate3'] = mkt_rate['index_rate'].shift(3)

# 股票涨幅数据
all_change_pivot = all_price.pivot('trade_dt', 's_info_windcode', 's_dq_close').pct_change()*100
all_change = all_change_pivot.stack().reset_index().rename(columns={0:'stocks_rate'})

# 数据汇总
sum_data = pd.merge(all_change, mkt_rate[['trade_dt', 'index_rate', 'index_rate1', 'index_rate2', 'index_rate3']], how='left').dropna(how='any')


def regress(data, y, x):
    Y = data[y]
    X = data[x]
    X['intercept'] = 1
    result = sm.OLS(Y, X).fit()
    return result.rsquared

sum_data['year_month'] = sum_data['trade_dt'].apply(lambda x: x[:6])
temp_sum_data = sum_data.dropna(how='any')

R_squared = temp_sum_data.groupby(['year_month', 's_info_windcode']).apply(regress, 'stocks_rate', ['index_rate']).reset_index().rename(columns={0:'R_1'})

t=time.time()
R_squared['R_3'] = temp_sum_data.groupby(['year_month', 's_info_windcode']).apply(regress, 'stocks_rate', ['index_rate', 'index_rate1', 'index_rate2', 'index_rate3']).values
print('compute_pricedelay running time:%10.4fs' % (time.time() - t))

R_squared['pricedelay']=1-(R_squared.R_1/R_squared.R_3)
result = pd.merge(sum_data, R_squared, how='left')


x2=pd.read_pickle(indir+'factor'+'/'+INDEX2+'_pricedelay.pkl')
x2[x2.s_info_windcode=='000001.SZ']
