import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
from bubble_model.useless.LPPLS_WYQ_old import data_in, lppls_compute


# 展示价格上涨中对数周期性和幂律增长特点
################################################################
def myfun(t, a, b, m):
    return a + b * np.power(t, m)

# 数据读入
dir = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\ht.xls'
# data_all = pd.read_excel(dir, skiprows=2348, skipfooter=2)
data_all = pd.read_excel(dir, skiprows=6062, skipfooter=17)

num = len(data_all)

# 数据整理
# data = data_all.iloc[:, 2:8:4].copy()  # 数据切片
data = data_all.copy()
data.columns = ['date', 'close']  # 列重命名

# y,n
ydata, n = data['close'].values, len(data)
xdata = np.linspace(1, n, n)

# 拟合幂律模型
bds = ([0, -np.inf, 0], [np.inf, 0, 1])
popt, pcov = curve_fit(myfun, num-xdata, ydata, bounds=bds, method='trf')

# 原始数据与拟合结果
f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(xdata, ydata)
ax.plot(xdata, myfun(num-xdata, *popt))
plt.show()

# y与y_hat之差
f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(xdata, ydata - myfun(num-xdata, *popt))
plt.show()

# y与y_hat之差 倒序
f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(xdata, (ydata - myfun(num-xdata, *popt))[::-1])
plt.show()

# y与y_hat之差 倒序 对数坐标
f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(np.log(xdata), (ydata - myfun(num-xdata, *popt))[::-1])
plt.show()

# 2015年股灾
import pandas as pd

look_bl = pd.read_excel(r'C:\Users\Administrator\Desktop\LPPL模型\15年股灾结果数据\0322result.xls')
look_bl.predict.hist(bins=100)
look_bl.predict[look_bl.predict<=40].hist(bins=100)

look_20140408_20150501 = pd.read_excel('C:\\Users\\Administrator\\Desktop\\' + 'result_wind全A.xls')
look_20140408_20150501.breakday_predict.hist(bins=100)

dir = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\wind.xls'
ydata1, n1 = data_in('20140408', '20150501', dir)
x1 = np.arange(1, n1+1, 1)
ydata2, n2 = data_in('20140408', '20151101', dir)
x2 = np.arange(1, n2+1, 1)

coef = look_20140408_20150501.iloc[0, :]
ydata_predict = lppls_compute(x2, coef[0], coef[1], coef[2], coef[3], coef[4], coef[5], coef[6])

f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(x2, ydata2, c='r', ls='--')
ax.plot(x1, ydata1, c='b', ls='-')
ax.plot(x2, ydata_predict, c='r', ls='-')
ax.vlines(x=coef[0], ymin=min(ydata2), ymax=coef[3], colors='#FFA500', linestyles='--')
plt.show()

# 顺丰控股 #################################################################################
import pandas as pd

look_bl = pd.read_excel('C:\\Users\\Administrator\\Desktop\\' + 'result_sfkg.xls')
look_bl.predict.hist(bins=100)

look_20191001_20201231 = pd.read_excel('C:\\Users\\Administrator\\Desktop\\' + 'result_sfkg顺丰控股.xls')
look_20191001_20201231.breakday_predict.hist(bins=100)

dir = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\sfkg.xls'
ydata1, n1 = data_in('20191001', '20201231', dir)
x1 = np.arange(1, n1+1, 1)
ydata2, n2 = data_in('20191001', '2021602', dir)
x2 = np.arange(1, n2+1, 1)

coef = look_20191001_20201231.iloc[1, :]
ydata_predict = lppls_compute(x2, coef[0], coef[1], coef[2], coef[3], coef[4], coef[5], coef[6])

f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(x2, ydata2, c='r', ls='--')
ax.plot(x1, ydata1, c='b', ls='-')
ax.plot(x2, ydata_predict, c='r', ls='-')
ax.vlines(x=coef[0], ymin=min(ydata2), ymax=coef[3]-2, colors='#FFA500', linestyles='--')
plt.show()

# 螺纹钢##########################################################################
import pandas as pd

look_bl = pd.read_excel('C:\\Users\\Administrator\\Desktop\\' + 'result_lwg.xls')
look_bl.predict.hist(bins=100)

look_20191001_20201231 = pd.read_excel('C:\\Users\\Administrator\\Desktop\\' + 'result_lwg螺纹钢.xls')
look_20191001_20201231.breakday_predict.hist(bins=100)

dir = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\lwg.xls'
ydata1, n1 = data_in('20200101', '20210401', dir)
x1 = np.arange(1, n1+1, 1)
ydata2, n2 = data_in('20200101', '20210603', dir)
x2 = np.arange(1, n2+1, 1)

coef = look_20191001_20201231.iloc[1, :]
ydata_predict = lppls_compute(x2, coef[0], coef[1], coef[2], coef[3], coef[4], coef[5], coef[6])

f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(x2, ydata2, c='r', ls='--')
ax.plot(x1, ydata1, c='b', ls='-')
ax.plot(x2, ydata_predict, c='r', ls='-')
ax.vlines(x=coef[0], ymin=min(ydata2), ymax=coef[3]-0.8, colors='#FFA500', linestyles='--')
plt.show()


# 越南指数 #################################################################################
import pandas as pd

look_bl = pd.read_excel('C:\\Users\\Administrator\\Desktop\\' + 'result_ynzs.xls')
look_bl.breakday_predict[look_bl.breakday_predict>0].hist(bins=100)

look_20191201_20210601 = pd.read_excel('C:\\Users\\Administrator\\Desktop\\' + 'result_ynzs越南.xls')
look_20191201_20210601.breakday_predict.hist(bins=100)

dir = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\ynzs.xls'
ydata1, n1 = data_in('20191201', '20210602', dir)
x1 = np.arange(1, n1+1, 1)
x2 = np.arange(1, n1+60, 1)

coef = look_20191201_20210601.iloc[20, :]
ydata_predict = lppls_compute(x2, coef[0], coef[1], coef[2], coef[3], coef[4], coef[5], coef[6])

f, ax = plt.subplots(1, 1, figsize=(6, 3))
ax.plot(x1, ydata1, c='b', ls='-')
ax.plot(x2, ydata_predict, c='r', ls='--')
ax.vlines(x=coef[0], ymin=min(ydata1), ymax=coef[3]+0.1, colors='#FFA500', linestyles='--')
plt.show()

