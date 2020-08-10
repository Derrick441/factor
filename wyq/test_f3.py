import pandas as pd
import numpy as np
import time
import statsmodels.api as sm
import sqlconn

INDEX = 'all'
indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'

# startdate = '20200701'
# enddate = '20200801'
#
# conn = sqlconn.sqlconn()
# t = time.time()
# sqlquery = 'select s_info_windcode,trade_dt,s_dq_mv,s_val_pe_ttm,s_val_pb_new,s_dq_freeturnover,s_dq_close_today' \
#            ' from wind.AShareEODDerivativeIndicator where trade_dt>= ' +startdate+ ' and trade_dt<= ' + enddate
# data = pd.read_sql(sqlquery, conn)
# print(time.time()-t)



# turnover = pd.read_pickle(indir + INDEX + '/' + INDEX + '_day_mv_pe_pb_turn_close.pkl')
# turnover.head()
# turnover.shape


# #收盘价数据查看
# data_price = pd.read_pickle(indir + INDEX + '/' + INDEX + '_band_dates_stocks_closep.pkl')
# data_price.shape

#日行情数据
data_all_band_price = pd.read_pickle(indir + INDEX + '/' + INDEX + '_band_price.pkl')
data_all_band_price.shape

#取A股成交量数据
data_volume = data_all_band_price[['trade_dt', 's_info_windcode', 's_dq_volume']]
data_volume.shape

#A股流通股本数据
data_fashre = pd.read_pickle(indir + INDEX + '/' + INDEX + '_float_a_shr.pkl')
data_fashre_reset = data_fashre.reset_index()
data_fashre_reset.head()

#A股流通市值数据
data_famv = pd.read_pickle(indir + INDEX + '/' + INDEX + '_float_a_mv.pkl')
data_famv_reset = data_famv.reset_index()
data_famv_reset.head()


#数据合并
data_sum = pd.merge(data_volume, data_fashre_reset, how='left')
data_sum = pd.merge(data_sum, data_famv_reset, how='left')

#换手率计算
data_sum['turnover'] = data_sum.s_dq_volume / data_sum.float_a_shr

#去除nan
temp_data_sum = data_sum.dropna(how='any')
#去除0
temp_data_sum = temp_data_sum[temp_data_sum.turnover!=0]
temp_data_sum = temp_data_sum[temp_data_sum.float_a_mv!=0]

#对数处理
temp_data_sum['ln_turnover'] = np.log(temp_data_sum.turnover)
temp_data_sum['ln_mv'] = np.log(temp_data_sum.float_a_mv)

#回归计算残差
def regress(data, y, x):
    Y = data[y]
    X = data[x]
    X['intercept'] = 1
    result = sm.OLS(Y, X).fit()
    return result.resid

#标记年月
temp_data_sum['year_month'] = temp_data_sum['trade_dt'].apply(lambda x: x[:6])

t = time.time()
temp = temp_data_sum[temp_data_sum.s_info_windcode=='000001.SZ'].groupby(['year_month', 's_info_windcode'])\
                    .apply(regress, 'ln_turnover', ['ln_mv'])\
                    .reset_index().rename(columns={0: 'turnover_adjusted'})
print(time.time()-t)

# #回归计算拟合值
# def regress1(data, y, x):
#     Y = data[y]
#     X = data[x]
#     X['intercept'] = 1
#     result = sm.OLS(Y, X).fit()
#     return result.fittedvalues
#
# temp = temp_data_sum[temp_data_sum['s_info_windcode']=='000001.SZ'].groupby(['year_month', 's_info_windcode']) \
#                                                                    .apply(regress1, 'ln_turnover', ['ln_mv']) \
#                                                                    .reset_index().rename(columns={0: 'turnover_adjusted'})

# result = pd.merge(data_sum, temp, how='left')
#
#
#
#
#
# from sklearn.linear_model import LinearRegression
#
# #回归计算残差
# def regress2(data, y, x):
#     Y = data[y]
#     X = data[x]
#     model = LinearRegression()
#     model.fit(X,Y)
#     return Y-model.predict(X)
#
# t=time.time()
# temp1 = temp_data_sum[temp_data_sum.s_info_windcode=='000001.SZ'].groupby(['year_month', 's_info_windcode'])\
#                     .apply(regress2, 'ln_turnover', ['ln_mv'])\
#                     .reset_index().rename(columns={0: 'turnover_adjusted'})
# print(time.time()-t)
#
#
#
#
#
#
#
#
#
# temp_data=temp_data_sum[temp_data_sum.s_info_windcode=='000001.SZ']
# t=time.time()
# temp1 = temp_data.groupby(['year_month', 's_info_windcode'])\
#                     .apply(regress, 'ln_turnover', ['ln_mv'])\
#                     .reset_index().rename(columns={0: 'turnover_adjusted'})
# print(time.time()-t)
#
#
#
# temp_data=temp_data_sum[(temp_data_sum.s_info_windcode=='000001.SZ') |
#                         (temp_data_sum.s_info_windcode=='000002.SZ') |
#                         (temp_data_sum.s_info_windcode=='000004.SZ') |
#                         (temp_data_sum.s_info_windcode=='000010.SZ')]
#
# t=time.time()
# temp2 = temp_data.groupby(['year_month', 's_info_windcode'])\
#                     .apply(regress, 'ln_turnover', ['ln_mv'])\
#                     .reset_index().rename(columns={0: 'turnover_adjusted'})
# print(time.time()-t)






temp_data_sum.trade_dt = temp_data_sum.trade_dt.astype(int)
temp_data_06=temp_data_sum[temp_data_sum.trade_dt <= 20100408]

t=time.time()
temp2 = temp_data_sum.groupby(['year_month', 's_info_windcode'])\
                    .apply(regress, 'ln_turnover', ['ln_mv'])\
                    .reset_index().rename(columns={0: 'turnover_adjusted'})
print(time.time()-t)


