import pandas as pd
import numpy as np
import time
np.seterr(invalid='ignore')

INDEX = 'all'
indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'

#股价数据导入
t=time.time()
price=pd.read_pickle(indir+INDEX+'/'+INDEX+'_band_dates_stocks_closep.pkl')
print(time.time()-t)

#股价数据pivot展开
t=time.time()
price_pivot=price.pivot('trade_dt','s_info_windcode','s_dq_close')
print(time.time()-t)

# #根据250个交易日涨跌幅滚动计算相关系数矩阵，从矩阵中找到距离每个股票最近的股票构成组合，计算组合平均价作为股票参考价
# t=time.time()
# ref_price_pivot=[]
# #price_pivot.shape[1]
# for i in range(620, 700):
#     print(price_pivot.index[i-1])
#     #提取250个交易日股价数据,剔除全部为nan的股票,填补部分为nan的数据
#     temp_price_na = price_pivot.iloc[i - 250:i, :].dropna(axis=1,how='all').copy().fillna(0)
#     #取当前期股价数据
#     temp_price_now=temp_price_na.iloc[-1,:]
#
#     #股价转涨幅
#     temp_change_na=(temp_price_na.pct_change()*100).dropna(axis=0, how='all').copy().fillna(0)
#     #计算相关系数矩阵，并将其转化为距离
#     temp_dist=1-temp_change_na.corr()
#
#     #取每列1%分位的距离
#     temp_quantile_dist=temp_dist.quantile(q=0.01, axis=0, numeric_only=True, interpolation='higher')
#     #标记每列距离小于1%分位的股票
#     temp_nearst=((temp_dist.values-temp_quantile_dist.values)<=0)+0
#     #计算股票参考价格
#     temp_mean=(np.dot(temp_price_now.values, temp_nearst)-temp_price_now.values)/(temp_nearst.sum(axis=0)-1)
#
#     #np.array格式转dataframe格式，并补充之前剔除的股票（自动填充为NaN）
#     temp_result=pd.DataFrame(temp_mean, index=temp_price_now.index, columns=[price_pivot.index[i-1]]).T.reindex(columns=price_pivot.columns)
#     #存放
#     ref_price_pivot.append(temp_result)
# print(time.time()-t)


t=time.time()
ref_price_pivot=[]
#price_pivot.shape[0]
for i in range(620, 700):
    print(price_pivot.index[i-1])
    #提取250个交易日股价数据,剔除含nan的股票
    temp_price_na = price_pivot.iloc[i - 250:i, :].dropna(axis=1,how='any')
    #取当前期股价数据
    temp_price_now=temp_price_na.iloc[-1,:]

    #股价转涨幅
    temp_change_na=(temp_price_na.pct_change()*100).dropna(how='any')
    #计算相关系数矩阵，并将其转化为距离
    temp_dist=pd.DataFrame(1-np.corrcoef(temp_change_na.T))

    #取每列1%分位的距离
    temp_quantile_dist=temp_dist.quantile(q=0.01, axis=0, numeric_only=True, interpolation='higher')
    #标记每列距离小于1%分位的股票
    temp_nearst=((temp_dist.values-temp_quantile_dist.values)<=0)+0
    #计算股票参考价格
    temp_mean=(np.dot(temp_price_now.values, temp_nearst)-temp_price_now.values)/(temp_nearst.sum(axis=0)-1)

    #np.array格式转dataframe格式，并补充之前剔除的股票（自动填充为NaN）
    temp_result=pd.DataFrame(temp_mean, index=temp_price_now.index, columns=[price_pivot.index[i-1]]).T.reindex(columns=price_pivot.columns)
    #存放
    ref_price_pivot.append(temp_result)
print(time.time()-t)


#合并
t=time.time()
ref_price=pd.concat(ref_price_pivot)
print(time.time()-t)

#数据格式调整
t=time.time()
ref_price_new=ref_price.unstack().reset_index().rename(columns={'level_1':'trade_dt', 0:'ref_price'})
print(time.time()-t)

#合并参考价格、股价数据
t=time.time()
data_sum=pd.merge(ref_price_new,price,how='left')
print(time.time()-t)



#计算对数价差
t=time.time()
data_sum['pricespread']=np.log(data_sum['s_dq_close'])-np.log(data_sum['ref_price'])
print(time.time()-t)

#计算对数价差60日均值和标准差
t=time.time()
data_sum['pricespread_60_mean']=data_sum.groupby(['s_info_windcode'])['pricespread'].rolling(60).mean().values
print(time.time()-t)

t=time.time()
data_sum['pricespread_60_std']= data_sum.groupby(['s_info_windcode'])['pricespread'].rolling(60).std().values
print(time.time()-t)

# 计算价差偏离度
time.time()
data_sum['spreadbias']=(data_sum['pricespread']-data_sum['pricespread_60_mean'])/data_sum['pricespread_60_std']
print(time.time()-t)

# 查看最终输出
x1=pd.read_pickle(indir+INDEX+'/'+INDEX+'_spreadbias.pkl')
x1[x1.s_info_windcode=='000001.SZ']
