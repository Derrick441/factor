import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
from sklearn import linear_model
from datetime import datetime
import matplotlib.pyplot as plt


# LPPL模型
def LPPL(t, a, b, c1, c2, m, w, tc):
    return a + np.power(tc - t, m) * (b + ((c1 * np.cos(w * np.log(tc - t))) + (c2 * np.sin(w * np.log(tc - t)))))


# 数据读入
def data_in(t1, t2, plot=False):
    dir = 'C:\\Users\\Administrator\\Desktop\\'
    data_all = pd.read_excel(dir+'wind.xls')

    # 数据整理
    data = data_all[['日期', '收盘价(元)']].copy()
    data = data.drop(data.tail(3).index)  # 数据切片、去除na
    data.columns = ['date', 'close']  # 列重命名

    # 数据截取
    sta = datetime.strptime(t1, '%Y%m%d')  # 起始结束时间
    end = datetime.strptime(t2, '%Y%m%d')
    data_1 = data[(data.date > sta) & (data.date < end)]

    # y
    ydata = np.log(data_1['close'].values)
    n = len(ydata)

    # 作图
    if plot:
        f, ax = plt.subplots(1, 1, figsize=(6, 3))
        ax.plot(data['date'], data['close'])  # 全部数据查看
        ax.plot(data_1['date'], data_1['close'])  # 测算数据查看
        plt.show()

        f, ax1 = plt.subplots(1, 1, figsize=(6, 3))
        xdata = np.linspace(0, n-1, n)
        ax1.plot(xdata, ydata)
        plt.show()

    return ydata, n


# 均方差
def MSE(Y, Yhat, p):
    return np.sum((Y-Yhat)**2) / np.float(len(Y) - p)


def myplot(x, y):
    f, ax = plt.subplots(1, 1, figsize=(6, 3))
    ax.plot(x, y, 'r-')
    plt.show()


# 寻找t1*
def find_t1(xdata, ydata, n, plot=False):
    brds = []
    tcs = []
    mses = []
    for i in range(n - 30):
        print(i)
        bds = ([0, -np.inf, -np.inf, -np.inf, 0.1, 2, n-(n-i)], [np.inf, np.inf, np.inf, np.inf, 0.9, 13, np.inf])
        y = ydata[i:]
        x = np.linspace(0, len(y)-1, len(y))
        try:
            popt, pcov = curve_fit(LPPL, x, y, bounds=bds, method='trf')
        except:
            popt = [np.nan] * 7
        mse = MSE(y, LPPL(x, *popt), 7)
        brds.append(len(x) + 30)
        tcs.append(popt[-1])
        mses.append(mse)

    if plot:
        myplot(xdata[:-30], mses)

    return pd.DataFrame({'breakday': brds, 'tc': tcs, 'mse': mses})


if __name__ == '__main__':

    # 数据准备
    ydata, n = data_in('20141001', '20150501')
    xdata = np.linspace(0, n-1, n)

    # 参数边界
    result = find_t1(xdata, ydata, n)
    myplot(xdata[:-30], result.mse)

    fun_reg = linear_model.LinearRegression()
    data_mses = pd.DataFrame({'date': xdata[:-30]-n+7, 'mses': result.mse}).dropna()
    x, y = data_mses.date.values.reshape(len(data_mses), 1), data_mses.mses.values.reshape(len(data_mses), 1)
    fun_l = fun_reg.fit(x, y)
    mses1 = data_mses['mses'] + fun_l.coef_[0]*(-data_mses['date'])
    mses1.plot()
    plt.show()

    result.to_excel('C:\\Users\\Administrator\\Desktop\\' + 'result_first.xls', index=False)
