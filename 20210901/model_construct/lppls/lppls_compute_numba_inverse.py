import numpy as np
import numba as nb
import pandas as pd
from datetime import datetime
import random
from scipy.optimize import minimize
import multiprocessing
import cx_Oracle
from matplotlib import pyplot as plt
import warnings
warnings.filterwarnings("ignore")


# excel读入数据及处理
def excel_read(dir_excel, columns_names, columns_names_new, t1, t2):
    # 数据读入
    data_all = pd.read_excel(dir_excel, skipfooter=2, engine='openpyxl')
    # 数据整理
    data = data_all[columns_names].copy()  # 数据切片
    data.columns = columns_names_new  # 列重命名
    # 数据截取
    sta = datetime.strptime(t1, '%Y%m%d')
    end = datetime.strptime(t2, '%Y%m%d')
    data_ = data[(data.date >= sta) & (data.date <= end)]
    print(data_.head(1), data_.tail(1))
    # 观察数据
    price_list = data_.close_adj.to_list()
    price_list = np.log(price_list)
    return price_list


# 核心拟合模型
@nb.jit(nopython=True)
def lppls_keyfun(t, m, w, a, b, c1, c2):
    x = t
    x_log = np.log(x)
    f = np.power(x, m)
    g = f * np.cos(w * x_log)
    h = f * np.sin(w * x_log)
    return a + b*f + c1*g + c2*h


# 线性参数由非线性参数表达
@nb.jit(nopython=True)
def matrix_equation(observations, m, w):
    # 数据
    X = observations[0]
    Y = observations[1]
    # 时间变量做调整
    x = X
    x_log = np.log(x)
    # 子函数部分表达
    f = np.power(x, m)
    g = f * np.cos(w * x_log)
    h = f * np.sin(w * x_log)
    # 数组堆叠
    A = np.stack((np.ones_like(x), f, g, h))
    # 最小化平方法估计线性参数
    return np.linalg.lstsq(A.T, Y, rcond=-1.0)[0]


# 优化器优化目标函数：均方差误
@nb.jit(nopython=True)
def lppls_mse(x, *args):
    # 非线性参数和观察数据
    m, w, obs = x[0], x[1], args[0]
    # 线性参数
    a, b, c1, c2 = matrix_equation(obs, m, w)
    # 真实值、预测值
    x, y, num = obs[0], obs[1], len(obs[0])
    y_ = np.zeros(num)
    for i in range(num):
        y_[i] = lppls_keyfun(x[i], m, w, a, b, c1, c2)
    # mse
    delta = np.subtract(y_, y)
    delta_2 = np.power(delta, 2)
    return np.sum(delta_2) / (num+7)


# 返回所需要的参数
@nb.jit(nopython=True)
def lppls_coefs(m, w, observations):
    # 4个线性参数
    a, b, c1, c2 = matrix_equation(observations, m, w)
    # 真实值、预测值
    x, y, num = observations[0], observations[1], len(observations[0])
    y_ = np.zeros(num)
    for i in range(num):
        y_[i] = lppls_keyfun(x[i], m, w, a, b, c1, c2)
    # mse
    delta = np.subtract(y_, y)
    delta_2 = np.power(delta, 2)
    mse = np.sum(delta_2) / (num + 7)
    # R2
    bias = y - np.mean(y)
    bias_2 = np.power(bias, 2)
    R2 = 1 - (np.sum(delta_2) / np.sum(bias_2))
    # 返回8个值
    return m, w, a, b, c1, c2, mse, R2


# 优化器寻找最优拟合参数
def lppls_minimize(init_coefs, observations, minimizer, init_limits):
    # 使用scipy的minimize计算出最小化mse的非线性参数
    coefs = minimize(fun=lppls_mse, x0=init_coefs, args=observations, method=minimizer, bounds=init_limits)
    # 计算非线性参数和mse、R2，并返回所需要的全部参数
    if coefs.success:
        m, w = coefs.x[0], coefs.x[1]
        return lppls_coefs(m, w, observations)
    else:
        raise UnboundLocalError


# lppls拟合主函数
def lppls_fit(max_searches, observations, minimizer, init_limits):
    # 禁忌搜索算法（在约束条件内，随机设定参数值，通过优化算法计算出”最优“参数值；若随机设定值无效，更改设定值再寻找）
    search_count = 0
    while search_count < max_searches:
        # 随机设定参数值
        non_lin_vals = [random.uniform(a[0], a[1]) for a in init_limits]
        init_coefs = np.array(non_lin_vals)
        # 优化算法计算”最优“参数值
        try:
            m, w, a, b, c1, c2, mse, R2 = lppls_minimize(init_coefs, observations, minimizer, init_limits)
            return m, w, a, b, c1, c2, mse, R2
        # 若无法找到”最优“参数值，search_count+1再次寻找
        except(np.linalg.LinAlgError, UnboundLocalError, ValueError):
            search_count += 1
    # 若n次寻找都无法找到”最优“参数值，返回0值
    return 0, 0, 0, 0, 0, 0, 0, 0


# 查看 拟合图
def draw_cheak_fit(result, y, freq, dir, stock, t1, t2):
    # 拟合参数
    coefs = result.iloc[0, 0:6]
    m, w, a, b, c1, c2 = coefs[0], coefs[1], coefs[2], coefs[3], coefs[4], coefs[5]
    # 设定x
    nums_1d = {'d': 1, '2h': 2, 'h': 4, '30m': 8, '15m': 16, '5m': 48}
    num = len(y) + 60*nums_1d[freq]
    x_new = np.linspace(1, num, num)
    # y预测值
    y_ = np.zeros(num)
    for i in range(num):
        y_[i] = lppls_keyfun(x_new[i], m, w, a, b, c1, c2)
    # 作图
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    ax.plot(y, c='b', ls='-')
    ax.plot(y_, c='r', ls='-')
    plt.savefig(dir+stock+'_'+t1+'_'+t2+'_bubble_inverse.png')


# 反泡沫主函数
def lppls_inverse_mian(dir_file, stock, t1, t2, freq):
    # 数据读入及整理
    excel_columns = ['代码', '日期', '收盘价(元)']
    excel_column_names = ['code', 'date', 'close_adj']
    y = excel_read(dir_file + stock + '.xlsx', excel_columns, excel_column_names, t1, t2)
    x = np.linspace(1, len(y), len(y))
    observations = np.array([x, y])

    # 计算设定
    compute_num = 1000
    max_searches = 100
    minimizer = 'SLSQP'
    init_limits = [
        (0.1, 0.9),  # m : 0.1 ≤ m ≤ 0.9
        (3, 15),  # ω : 6 ≤ ω ≤ 13
    ]

    # 拟合
    t = datetime.now()
    result = []
    for i in range(compute_num):  # ***
        temp = lppls_fit(max_searches, observations, minimizer, init_limits)
        result.append(list(temp))
    print(datetime.now() - t)

    # 结果整理
    result_temp = pd.DataFrame(result, columns=['m', 'w', 'a', 'b', 'c1', 'c2', 'mse', 'R2'])
    result_temp.sort_values(by=['mse'], ascending=True, inplace=True)

    # 作图查看
    draw_cheak_fit(result_temp, y, freq, dir_file, stock, t1, t2)

    return result_temp


if __name__ == '__main__':
    # 迁移设置
    dir_file = 'C:\\Users\\Administrator\\Desktop\\LPPLS\\'

    # stock = 'hskj'
    # t1 = '20210628'
    # t2 = '20210731'
    # freq = '5m'
    # result = lppls_inverse_mian(dir_file, stock, t1, t2, freq)

    # 输入信息
    stock, t1, t2, freq = input('股票    开始      结束      频率\n').split()
    t = datetime.now()
    print('计算开始:')
    result = lppls_inverse_mian(dir_file, stock, t1, t2, freq)
    print('计算结束', datetime.now() - t)


