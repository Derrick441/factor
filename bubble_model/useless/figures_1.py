import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from bubble_model.useless.LPPLS_WYQ_old import lppls_compute


if __name__ == '__main__':
    t1, t2 = '20130101', '20150501'
    t3, t4 = '20141201', '20150525'
    t5, t6 = '20150501', '20151101'
    t7, t8 = '20150525', '20151101'

    data_all = pd.read_excel('C:\\Users\\Administrator\\Desktop\\' + 'wind.xls')

    # 数据整理
    data = data_all[['日期', '收盘价(元)']].copy() # 数据切片
    data = data.drop(data.tail(3).index)  # 去除na
    data.columns = ['date', 'close']  # 列重命名

    # 数据截取1
    sta = datetime.strptime(t1, '%Y%m%d')  # 起始结束时间
    end = datetime.strptime(t2, '%Y%m%d')
    data_1 = data[(data.date > sta) & (data.date < end)]

    # 数据截取2
    sta = datetime.strptime(t3, '%Y%m%d')  # 起始结束时间
    end = datetime.strptime(t4, '%Y%m%d')
    data_2 = data[(data.date > sta) & (data.date < end)]

    # 数据截取3
    sta = datetime.strptime(t5, '%Y%m%d')  # 起始结束时间
    end = datetime.strptime(t6, '%Y%m%d')
    data_3 = data[(data.date > sta) & (data.date < end)]

    # 数据截取4
    sta = datetime.strptime(t7, '%Y%m%d')  # 起始结束时间
    end = datetime.strptime(t8, '%Y%m%d')
    data_4 = data[(data.date > sta) & (data.date < end)]

    # 作图
    f, ax = plt.subplots(1, 1, figsize=(6, 3))
    ax.plot(data_1['date'], data_1['close'], c='b', ls='-')
    ax.plot(data_2['date'], data_2['close'], c='r', ls='--')
    ax.plot(data_4['date'], data_4['close'], c='g', ls='--')
    plt.xticks(fontsize=6)
    plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(110))
    plt.show()

    f1, ax1 = plt.subplots(1, 1, figsize=(6, 3))
    ax1.plot(data_1['date'], data_1['close'], c='b', ls='-')
    ax1.plot(data_3['date'], data_3['close'], c='r', ls='--')
    plt.xticks(fontsize=6)
    plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(120))
    plt.show()

    data_result = pd.read_excel('C:\\Users\\Administrator\\Desktop\\LPPL模型\\' + '0322result_.xls')
    f2, ax2 = plt.subplots(1, 1, figsize=(6, 3))
    plt.hist(data_result.err, bins=150, color="#FF0000", alpha=.7)
    plt.show()

    # 数据截取x
    tx1, tx2 = '20130101', '20151101'
    sta = datetime.strptime(tx1, '%Y%m%d')  # 起始结束时间
    end = datetime.strptime(tx2, '%Y%m%d')
    data_x = data[(data.date > sta) & (data.date < end)]

    y = np.log(data_x.close[300:])
    x = np.linspace(0, len(y)-1, len(y))

    data_coefs = pd.read_excel('C:\\Users\\Administrator\\Desktop\\LPPL模型\\' + '0322result.xls')
    coef = data_coefs.iloc[300, :9].to_list()
    y_ = lppls_compute(x, coef[2], coef[3], coef[4], coef[5], coef[6], coef[7], coef[8],)

    f3, ax3 = plt.subplots(1, 1, figsize=(6, 3))
    ax3.plot(x, y, c='b', ls='-')
    ax3.plot(x, y_, c='r', ls='--')
    ax3.vlines(x=275, ymin=min(y), ymax=coef[5], colors='#FFA500', linestyles='--')
    plt.show()
