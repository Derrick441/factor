import numpy as np
import pandas as pd


# 数据读入
dir = 'C:\\Users\\Administrator\\Desktop\\data_20170101_20180601_.pkl.pkl'
data = pd.read_pickle(dir)
print('data memory:', np.sum(data.memory_usage()) / (1024 ** 3))

# 去除新股（3个月），加入nh240_mean120之后无效
temp_data_ = data[~np.isnan(data.ma060)].copy()

# 数据划分两段、分别计算IC
temp_data = temp_data_.sort_values(by=['date'])

mid = int(temp_data.shape[0] / 2)
cols = np.r_[6, 11:temp_data.shape[1]]
corr1 = temp_data.iloc[:mid, cols].corr()
corr2 = temp_data.iloc[mid:, cols].corr()

ic = corr1['ret005'].to_frame().rename(columns={'ret005': 'first'})
ic['last'] = corr2['ret005']

# 根据IC筛选因子
thres = 0.01
ic_1 = ic[(ic['first'] >= thres) | (ic['first'] <= -thres)]
ic_2 = ic_1[(ic_1['last'] >= thres) | (ic_1['last'] <= -thres)]

# 根据显著因子，筛选股票，计算收益
temp_data.ret005.mean() / 5

# data_ = temp_data.copy()
# data_ = temp_data[temp_data.sp120 > 0].copy()
# data_['group'] = pd.cut(data_.sp120.rank(), 30, labels=range(30))
# result = data_.groupby('group')['ret005'].mean()
# result.plot(kind='bar', color='r')
# data_.sp120.quantile(0.1)
# data_.sp120.quantile(0.2)
# data_.sp120.quantile(0.3)
# data_.sp120.quantile(0.4)
# data_.sp120.quantile(0.5)
# data_.sp120.quantile(0.6)
# data_.sp120.quantile(0.7)
# data_.sp120.quantile(0.8)
# data_.sp120.quantile(0.9)
#
# data1 = temp_data[(temp_data.sp120 > 10.0)]
# temp_data.ret005.mean() / 5
# data1.ret001.mean()
# data1.ret005.mean() / 5
# data1.ret010.mean() / 10
# data1.ret020.mean() / 20

# data_ = temp_data.copy()
data_ = temp_data[temp_data.sp010 > 0].copy()
data_['group'] = pd.cut(data_.sp010.rank(), 30, labels=range(30))
result = data_.groupby('group')['ret005'].mean()
result.plot(kind='bar', color='r')

data_.sp010.quantile(0.1)
data_.sp010.quantile(0.2)
data_.sp010.quantile(0.3)
data_.sp010.quantile(0.4)
data_.sp010.quantile(0.5)
data_.sp010.quantile(0.6)
data_.sp010.quantile(0.7)
data_.sp010.quantile(0.8)
data_.sp010.quantile(0.9)

temp_data.ret005.mean() / 5
data1 = temp_data[(temp_data.sp010 > 20.0) & (temp_data.ret001 < 0.09)]
data1.ret001.mean()
data1.ret005.mean() / 5
data1.ret010.mean() / 10
data1.ret020.mean() / 20

data2 = temp_data[(temp_data.sp010 > 20.0) & (temp_data.ret001 < 0.09) & (temp_data.sp120 > 0.0)]
temp_data.ret005.mean() / 5
data2.ret001.mean()
data2.ret005.mean() / 5
data2.ret010.mean() / 10
data2.ret020.mean() / 20
