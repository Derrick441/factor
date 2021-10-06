import pandas as pd
import numpy as np
from bubble_model.useless.LPPLS_WYQ_old import data_in, lppls_compute
from matplotlib import pyplot as plt


# 移动起始时间 ******************************************************************
t1 = '20190101'
t2 = '20210610'
file_dir1 = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\舍得酒业复权数据(1).xls'
file_dir2 = 'C:\\Users\\Administrator\\Desktop\\result_sd_t1.xls'

# 数据读入
ydata, n, tdata = data_in(t1, t2, file_dir1)
result = pd.read_excel(file_dir2)

# 结果查看
result.breakday_predict.plot()
result.breakday_predict.hist(bins=100)
coef = result.iloc[20, :]

# 实际和预测值
x1 = np.linspace(1, int(coef[0]), int(coef[0]))
y1 = ydata[-int(coef[0]):]
x2 = np.linspace(1, n + 200, n + 200)
y2 = lppls_compute(x2, coef[3], coef[4], coef[5], coef[6], coef[7], coef[8], coef[9])

# 作图展示
f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(x1, y1, c='b', ls='-')
ax.plot(x2, y2, c='r', ls='--')
ax.vlines(x=coef[3], ymin=min(y1) * 0.9, ymax=max(y2) * 1.1, colors='#FFA500', linestyles='--')
plt.show()


# 移动结束时间 **********************************************************************************
t1 = '20200320'
t2 = '20210610'
file_dir1 = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\舍得酒业复权数据(1).xls'
file_dir2 = 'C:\\Users\\Administrator\\Desktop\\result_sd_t2.xls'

# 数据读入
ydata, n, tdata = data_in(t1, t2, file_dir1)
result = pd.read_excel(file_dir2)

# 结果处理
result['price'] = ydata[30:]
result['price_1'] = result[result['breakday_predict'] <= 2]['price']
result['price_2'] = result[result['breakday_predict'] >= 20]['price']

# 作图展示
f, ax = plt.subplots(1, 1, figsize=(10, 6))
ax.plot(result.endday, result.price, c='b', ls='-')
ax.plot(result.endday, result.price_1, c='r', ls='-')
ax.plot(result.endday, result.price_2, c='g', ls='-')
plt.show()

# 固定时间点*************************************************************************
t1 = '20200320'
t2 = '20210607'
file_dir1 = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\舍得酒业复权数据(1).xls'
file_dir2 = 'C:\\Users\\Administrator\\Desktop\\result_sd_fix_' + t1 + '_' + t2 + '.xls'

# 数据读入
ydata, n, tdata = data_in(t1, t2, file_dir1)
result = pd.read_excel(file_dir2)

# 结果查看
result.breakday_predict.plot()
result.breakday_predict.hist(bins=100)
coef = result.iloc[0, :]

# 实际和预测值
x1 = np.linspace(1, int(coef[0]), int(coef[0]))
y1 = ydata[-int(coef[0]):]
x2 = np.linspace(1, n+200, n+200)
y2 = lppls_compute(x2, coef[0], coef[1], coef[2], coef[3], coef[4], coef[5], coef[6])

# 作图展示
f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(x1, y1, c='b', ls='-')
ax.plot(x2, y2, c='r', ls='--')
ax.vlines(x=coef[0], ymin=min(y1)*0.9, ymax=max(y2)*1.1, colors='#FFA500', linestyles='--')
plt.show()

###########################################################################################


# 移动起始时间 ******************************************************************
t1 = '20180901'  # 全部数据起始时间
t2 = '20210630'  # 全部数据结束时间
file_dir1 = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\中颖电子.xls'
file_dir2 = 'C:\\Users\\Administrator\\Desktop\\result_中颖电子_t1.xls'

# 数据读入
ydata, n, tdata = data_in(t1, t2, file_dir1)
result_ = pd.read_excel(file_dir2)
result = result_[result_.breakday_predict>0]

# 结果查看
y1 = result.breakday_predict
y2 = result.R2
x = np.linspace(1, len(y1), len(y2))

f, ax1 = plt.subplots(1, 1, figsize=(6, 3))
ax1.plot(x, y1, c='b', ls='-')
ax2 = ax1.twinx()
ax2.plot(x, y2, c='r', ls='--')
plt.show()

data_1 = result[result.R2 >= 0]
data_1.breakday_predict.hist(bins=100)

data_2 = result[result.R2 >= 0.95]
data_2.breakday_predict.hist(bins=100)
coef = result.iloc[-100, :]

# 实际和预测值
x1 = np.linspace(1, int(coef[0]), int(coef[0]))
y1 = ydata[-int(coef[0]):]
x2 = np.linspace(1, n + 200, n + 200)
y2 = lppls_compute(x2, coef[3], coef[4], coef[5], coef[6], coef[7], coef[8], coef[9])

# 作图展示
f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(x1, y1, c='b', ls='-')
ax.plot(x2, y2, c='r', ls='--')
ax.vlines(x=coef[3], ymin=min(y1) * 0.9, ymax=max(y2) * 1.1, colors='#FFA500', linestyles='--')
plt.show()

# 移动结束时间 **********************************************************************************
t1 = '20180901'  # 全部数据起始时间
t2 = '20210630'  # 全部数据结束时间
file_dir1 = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\中颖电子.xls'
file_dir2 = 'C:\\Users\\Administrator\\Desktop\\result_中颖电子_t2.xls'

# 数据读入
ydata, n, tdata = data_in(t1, t2, file_dir1)
result_ = pd.read_excel(file_dir2)

# 结果处理
result_['price'] = ydata[30:]
# result = result_[result_.R2 > 0].copy()
result = result_.copy()
result['price_1'] = result[result['breakday_predict'] <= 3]['price']
result['price_2'] = result[result['breakday_predict'] >= 3]['price']

# 作图展示
f, ax = plt.subplots(1, 1, figsize=(10, 6))
ax.plot(result.endday, result.price, c='b', ls='-')
ax.plot(result.endday, result.price_1, c='r', ls='-')
ax.plot(result.endday, result.price_2, c='g', ls='-')
plt.show()


import pandas as pd
dir = 'C:\\Users\\Administrator\\Desktop\\20210713\\002812t1.xlsx'
data = pd.read_excel(dir, engine='openpyxl')
data_ = data[(data.breakday_predict>0)&(data.breakday_predict<100)]
data_.breakday_predict.hist(bins=100)