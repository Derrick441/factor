import numpy as np
import numba as nb
import pandas as pd
from datetime import datetime
import random
from scipy.optimize import minimize
import multiprocessing
from matplotlib import pyplot as plt
import warnings
warnings.filterwarnings("ignore")


# 核心拟合模型
@nb.jit(nopython=True)
def lppls_keyfun(t, tc, m, w, a, b, c1, c2):
    x = tc - t
    x_log = np.log(x)
    f = np.power(x, m)
    g = f * np.cos(w * x_log)
    h = f * np.sin(w * x_log)
    return a + b*f + c1*g + c2*h


# 线性参数由非线性参数表达
@nb.jit(nopython=True)
def matrix_equation(obs, tc, m, w):
    # 数据
    X = obs[0]
    Y = obs[1]
    # 时间变量做调整
    x = tc - X
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
    tc, m, w, obs = x[0], x[1], x[2], args[0]
    # 线性参数
    a, b, c1, c2 = matrix_equation(obs, tc, m, w)
    # 真实值、预测值
    x, y, num = obs[0], obs[1], obs.shape[1]
    y_ = np.zeros(num)
    for i in range(num):
        y_[i] = lppls_keyfun(x[i], tc, m, w, a, b, c1, c2)
    # mse
    delta = np.subtract(y_, y)
    delta_2 = np.power(delta, 2)
    return np.sum(delta_2) / (num+7)


# 返回所需要的参数
@nb.jit(nopython=True)
def lppls_coefs(tc, m, w, obs):
    # 4个线性参数
    a, b, c1, c2 = matrix_equation(obs, tc, m, w)
    # 真实值、预测值
    x, y, num = obs[0], obs[1], obs.shape[1]
    y_ = np.zeros(num)
    for i in range(num):
        y_[i] = lppls_keyfun(x[i], tc, m, w, a, b, c1, c2)
    # mse
    delta = np.subtract(y_, y)
    delta_2 = np.power(delta, 2)
    mse = np.sum(delta_2) / (num + 7)
    # R2
    bias = y - np.mean(y)
    bias_2 = np.power(bias, 2)
    R2 = 1 - (np.sum(delta_2) / np.sum(bias_2))
    # 返回9个值
    return tc, m, w, a, b, c1, c2, mse, R2


# 优化器寻找最优参数
def lppls_minimize(init_coefs, observations, minimizer, coefs_limits):
    # 使用scipy的minimize计算最小化mse的非线性参数
    coefs = minimize(fun=lppls_mse, x0=init_coefs, args=observations, method=minimizer, bounds=coefs_limits)
    # 计算非线性参数和mse、R2，并返回所需要的全部参数
    if coefs.success:
        tc, m, w = coefs.x[0], coefs.x[1], coefs.x[2]
        return lppls_coefs(tc, m, w, observations)
    else:
        raise UnboundLocalError


# lppls拟合
def lppls_fit(max_searches, observations, minimizer, coefs_limits):
    # 禁忌搜索算法（在约束条件内，随机设定参数值，通过优化算法计算出”最优“参数值；若随机设定值无效，更改设定值再寻找）
    search_count = 0
    while search_count < max_searches:
        # 随机设定参数值
        non_lin_vals = [random.uniform(a[0], a[1]) for a in coefs_limits]
        init_coefs = np.array(non_lin_vals)
        # 优化算法计算”最优“参数值
        try:
            tc, m, w, a, b, c1, c2, mse, R2 = lppls_minimize(init_coefs, observations, minimizer, coefs_limits)
            return tc, m, w, a, b, c1, c2, mse, R2
        # 若无法找到”最优“参数值，search_count+1再次寻找
        except(np.linalg.LinAlgError, UnboundLocalError, ValueError):
            search_count += 1
    # 若n次寻找都无法找到”最优“参数值，返回0值
    return 0, 0, 0, 0, 0, 0, 0, 0, 0


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
    # 观察数据
    price_list = data_.close_adj.to_list()
    price_list = np.log(price_list)
    # 时间标签
    date_list = data_.date.to_list()
    return price_list, date_list


# 全部时间点：需要遍历的
def range_get(mode, num_obs, num_keep, bl):
    if mode == 't1':
        return range(0, num_obs-num_keep, bl)
    else:
        return range(num_keep, num_obs, bl)


# 单个时间点：数据切片
def obs_date_get(mode, price_list, date_list, multi_i):
    if mode == 't1':
        y_temp = price_list[multi_i:]
        x_temp = np.linspace(1, len(y_temp), len(y_temp))
        obs_temp = np.array([x_temp, y_temp])
        date_temp = date_list[multi_i:]
        obs_num = len(y_temp)
        return obs_temp, date_temp, obs_num
    else:
        y_temp = price_list[:multi_i]
        x_temp = np.linspace(1, len(y_temp), len(y_temp))
        obs_temp = np.array([x_temp, y_temp])
        date_temp = date_list[:multi_i]
        obs_num = len(y_temp)
        return obs_temp, date_temp, obs_num


# 单个时间点：lppls拟合拟合n次
def compute(percent, observations, date_list, multi_i, bl, num_once, max_searches, minimizer, coefs_limits):
    t = datetime.now()

    # 拟合模型
    result = []
    for i in range(num_once):                                                                            # ***
        temp = lppls_fit(max_searches, observations, minimizer, coefs_limits)
        result.append(list(temp))
    # 数据整合
    result_temp = pd.DataFrame(result, columns=['tc', 'm', 'w', 'a', 'b', 'c1', 'c2', 'mse', 'R2'])

    # x和y
    x = observations[0]
    y = observations[1]

    # 数据筛选
    condition1 = result_temp.a < (max(y)+2)
    condition2 = result_temp.b < 0
    result_ = result_temp[condition1 & condition2].copy()

    # 排序
    result_.sort_values(by=['mse'], ascending=True, inplace=True)
    # print(result_.shape)
    time_result = pd.Series([date_list[0], date_list[-1], len(y)], index=['start_time', 'end_time', 'num_obs'])
    print('{} {:.2%}'.format(int(multi_i/bl), percent), date_list[0], date_list[-1], datetime.now() - t)
    try:
        model_result = result_.iloc[0, :].copy()
        return time_result.append(model_result)
    except:
        model_result = pd.Series([np.nan] * 9, index=['tc', 'm', 'w', 'a', 'b', 'c1', 'c2', 'mse', 'R2'])
        return time_result.append(model_result)


# 遍历全部时间点
def lppls_main(dir_file, stock, t1, t2, mode, freq, bl):
    # ****************************************************************************************************************
    # t = datetime.now()

    # 参数读入
    lppls_coefs = pd.read_table(dir_file + 'lppls_coefs.txt', usecols=[0, 1, 2, 3], sep=',', header=None, index_col=0)
    # 参数设定
    columns_names = lppls_coefs.loc['columns_names'].to_list()  # excel中的列名：默认代码,日期,收盘价(元)
    columns_names_new = lppls_coefs.loc['columns_names_new'].to_list()  # 数据列名重命名：默认code,date,close_adj
    keep = float(lppls_coefs.loc['keep'].to_list()[0])  # 保留的天数：默认全部时间的20%（0.2）
    process_num = int(lppls_coefs.loc['process_num'].to_list()[0])  # 使用cpu数：默认3
    num_once = int(lppls_coefs.loc['num_once'].to_list()[0])  # 单个时间点最大计算数: 默认100
    max_searches = int(lppls_coefs.loc['max_searches'].to_list()[0])  # 单次计算最大搜寻数：默认100
    minimizer = lppls_coefs.loc['minimizer'].to_list()[0]  # 优化算法：默认SLSQP
    m_limits = tuple([float(x) for x in lppls_coefs.loc['m_limits'].to_list()[:2]])  # 默认文献m : 0.1 ≤ m ≤ 0.9
    w_limits = tuple([float(x) for x in lppls_coefs.loc['w_limits'].to_list()[:2]])  # 默认文献ω : 6 ≤ ω ≤ 13（有变动）
    bl = int(bl)  # 间隔数

    # print('参数设定:', datetime.now() - t)

    # ****************************************************************************************************************
    # t = datetime.now()

    # 观察数据读入
    price_list, date_list = excel_read(dir_file+stock+'.xlsx', columns_names, columns_names_new, t1, t2)
    # 观察数据总数
    num_obs = len(price_list)
    # 一天数据量
    nums_1d = {'d': 1, '2h': 2, 'h': 4, '30m': 8, '15m': 16, '5m': 48}
    num_1d = nums_1d[freq]
    # 观察数据保留量
    num_keep = int(num_obs * keep)
    # 遍历时间点
    multi_range = range_get(mode, num_obs, num_keep, bl)

    # print('数据读入及处理:', datetime.now() - t)

    # ****************************************************************************************************************
    # t = datetime.now()

    # 多进程计算
    pool = multiprocessing.Pool(processes=process_num)
    result_tra = []
    count = 0
    for multi_i in multi_range:
        count += 1
        percent = count / len(multi_range)
        # 数据切片
        obs_temp, date_temp, obs_num = obs_date_get(mode, price_list, date_list, multi_i)
        # 参数范围
        tc_limits = (obs_num + 0.01, obs_num + 250 * num_1d)  # tc : 崩盘时间暂设定为截止时间后0.01-250天
        coefs_limits = [tc_limits, m_limits, w_limits]
        # 每日遍历
        result_temp = pool.apply_async(compute, (percent, obs_temp, date_temp, multi_i, bl, num_once, max_searches, minimizer, coefs_limits))
        result_tra.append(result_temp)
    pool.close()
    pool.join()

    # print('多进程遍历计算:', datetime.now() - t)

    # ****************************************************************************************************************
    # t = datetime.now()

    # 结果提取
    result_get = []
    for i in range(len(result_tra)):
        result_get.append(result_tra[i].get())
    # 结果整合
    result = pd.concat(result_get, axis=1)
    result = result.T
    result.reset_index(drop=True, inplace=True)
    # 崩盘剩余时间
    result['breaktime_predict'] = (result['tc'] - result['num_obs'])
    result['breakday_predict'] = (result['tc'] - result['num_obs']) / num_1d
    # 结果输出
    result.to_excel(dir_file + stock + '_' + mode + '_' + t1 + '_' + t2 + '.xlsx', index=False)

    # print('计算结果整理及输出:', datetime.now() - t)

    # ****************************************************************************************************************
    # t = datetime.now()

    y = price_list
    x = np.linspace(1, len(y), len(y))
    observations = np.array([x, y])
    fig_draw(observations, mode, num_keep, result, bl)

    # print('作图及输出:', datetime.now() - t)
    return 0


# 查看 拟合图
def fig_draw(observations, mode, num_keep, result, bl):
    # 保存地址
    save_dir = dir_file + stock + '_' + mode + '_' + t1 + '_' + t2 + '_pictures.png'

    # 数据处理
    obs = pd.DataFrame(observations.T, columns=['x', 'y'])
    obs.reset_index(inplace=True)
    if mode == 't1':
        obs['flag'] = obs[obs.index <= (obs.shape[0]-num_keep)]['y']
        obs['num'] = obs.shape[0] - obs[obs.index <= (obs.shape[0]-num_keep)]['index']
    else:
        obs['flag'] = obs[obs.index >= num_keep]['y']
        obs['num'] = obs[obs.index >= num_keep]['index'] + 1

    result.reset_index(inplace=True)
    if mode == 't1':
        temp_x = result.index.to_list()
        fig3_x = temp_x + [x+temp_x[-1] for x in range(int(num_keep/bl))]
        fig3_break = result.breakday_predict.to_list() + [0 for x in range(int(num_keep/bl))]
        fig3_r2 = result.R2.to_list() + [0 for x in range(int(num_keep/bl))]
    else:
        result['index_'] = result['index'] + int(num_keep/bl)
        fig3_x = [x for x in range(int(num_keep/bl))] + result.index_.to_list()
        fig3_break = [0 for x in range(int(num_keep/bl))] + result.breakday_predict.to_list()
        fig3_r2 = [0 for x in range(int(num_keep/bl))] + result.R2.to_list()

    # 作图
    fig, ax = plt.subplots(2, 2, figsize=(40, 26))
    # fig1
    ax[0][0].plot(obs.x, obs.y, c='b', ls='-')
    ax[0][0].plot(obs.x, obs.flag, c='r', ls='-')
    # fig2
    ax[0][1].bar(range(len(obs.num)), obs.num)
    # fig3
    l1 = ax[1][0].plot(fig3_x, fig3_break, c='r', ls='-', label='break')
    temp_ax = ax[1][0].twinx()
    l2 = temp_ax.plot(fig3_x, fig3_r2, c='b', ls='-', label='R2')
    plt.legend(l1 + l2, ['break', 'R2'])
    # fig4
    if mode == 't1':
        ax[1][1].hist(result.breakday_predict, bins=100, label='break')
    else:
        if bl == 1:
            l1 = ax[1][1].plot(fig3_x, fig3_break, c='r', ls='-', label='break')
            temp_ax = ax[1][1].twinx()
            l2 = temp_ax.plot(fig3_x, obs.y, c='b', ls='-', label='y')
            plt.legend(l1 + l2, ['break', 'y'])

    # 输出
    plt.savefig(save_dir)

    # coefs = result.iloc[0, 0:7]
    # tc, m, w, a, b, c1, c2 = coefs[0], coefs[1], coefs[2], coefs[3], coefs[4], coefs[5], coefs[6]
    # nums_1d = {'d': 1, '2h': 2, 'h': 4, '30m': 8, '15m': 16, '5m': 48}
    # num = len(y) + 60*nums_1d[freq]
    # x_new = np.linspace(1, num, num)
    # y_ = np.zeros(num)
    # for i in range(num):
    #     y_[i] = lppls_keyfun(x_new[i], tc, m, w, a, b, c1, c2)
    # f, ax1 = plt.subplots(1, 1, figsize=(8, 5))
    # ax.plot(y, c='b', ls='-')
    # ax.plot(y_, c='r', ls='-')
    # plt.savefig(dir+stock+'_'+t1+'_'+t2+'_fit.png')


if __name__ == '__main__':
    # 迁移设置
    dir_file = 'C:\\Users\\Administrator\\Desktop\\20210819\\'

    # 计算
    stock, t1, t2, mode, freq, bl = input('股票    开始      结束     模式 频率 间隔\n').split()
    t = datetime.now()
    print('计算开始:')
    lppls_main(dir_file, stock, t1, t2, mode, freq, bl)
    print('计算结束', datetime.now() - t)

    # 是否继续
    again = input('\n是否继续？（yes or no）\n')
    while again == 'yes':
        # 计算
        stock, t1, t2, mode, freq, bl = input('\n股票    开始      结束     模式 频率 间隔\n').split()
        t = datetime.now()
        print('计算开始:')
        lppls_main(dir_file, stock, t1, t2, mode, freq, bl)
        print('计算结束', datetime.now() - t)
        # 是否继续
        again = input('\n是否继续？（yes or no）\n')
