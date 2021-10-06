import pandas as pd
import numpy as np
from datetime import datetime
from matplotlib import pyplot as plt
import random
from scipy.optimize import minimize
from sklearn import linear_model
import multiprocessing


# LPPLS模型估计算法（禁忌搜索+SLSQP）
class LPPLS(object):

    def __init__(self, observations):
        # 传入观察数据（x,y）
        self.observations = observations
        # 设定参数约束条件（3个）
        self.init_limits = [
            (self.observations[0, -1], self.observations[0, -1] * 2),  # tc : Critical Time
            (0.1, 0.9),  # m : 0.1 ≤ m ≤ 0.9
            (3, 15)  # ω : 6 ≤ ω ≤ 13
        ]

    # lppls模型公式
    def lppls(self, t, tc, m, w, a, b, c1, c2):
        return a + np.power(tc - t, m) * (b + ((c1 * np.cos(w * np.log(tc - t))) + (c2 * np.sin(w * np.log(tc - t)))))

    # 线性参数由非线性参数表达（给出非线性参数，计算出线性参数）
    def matrix_equation(self, observations, tc, m, w):
        # 观察数据（x,y：时间，价格）
        T = observations[0]
        P = observations[1]
        # 时间变量做调整
        tc_t = tc - T
        log_tc_t = np.log(tc_t)
        # 子函数部分表达
        ft = np.power(tc_t, m)
        gt = ft * np.cos(w * log_tc_t)
        ht = ft * np.sin(w * log_tc_t)
        # 数组堆叠
        A = np.stack((np.ones_like(tc_t), ft, gt, ht))
        # 最小化平方法估计线性参数
        return np.linalg.lstsq(A.T, P, rcond=-1.0)[0]

    # 均方差误
    def mse(self, x, *args):
        # 非线性参数和观察数据（x,y)
        tc, m, w, obs = x[0], x[1], x[2], args[0]
        # 线性参数
        a, b, c1, c2 = self.matrix_equation(obs, tc, m, w)
        # 预测值（y_）
        y_ = [self.lppls(t, tc, m, w, a, b, c1, c2) for t in obs[0, :]]
        # 均方差误
        delta = np.subtract(y_, obs[1, :])
        delta_s = np.power(delta, 2)
        return np.sum(delta_s) / (len(obs)+7)

    def minimize_lppls(self, init_coefs, minimizer):
        # 使用scipy的minimize计算出最小化mse的非线性参数
        cofs = minimize(fun=self.mse, x0=init_coefs, args=self.observations, method=minimizer, bounds=self.init_limits)
        # 计算非线性参数和mse、R2
        if cofs.success:
            # 全部7个参数
            tc, m, w = cofs.x[0], cofs.x[1], cofs.x[2]
            a, b, c1, c2 = self.matrix_equation(self.observations, tc, m, w)
            # mse
            y_ = [self.lppls(t, tc, m, w, a, b, c1, c2) for t in self.observations[0, :]]
            delta = np.subtract(y_, self.observations[1, :])
            delta_s = np.power(delta, 2)
            mse = np.sum(delta_s) / (len(self.observations) + 7)
            # R2
            bias = self.observations[1, :] - np.mean(self.observations[1, :])
            bias_s = np.power(bias, 2)
            R2 = 1 - (np.sum(delta_s) / np.sum(bias_s))
            # 返回这9个值
            return tc, m, w, a, b, c1, c2, mse, R2
        else:
            raise UnboundLocalError

    def fit(self, max_searches=25, minimizer='SLSQP'):
        # 禁忌搜索算法（在约束条件内，随机设定参数值，通过优化算法计算出”最优“参数值；若随机设定值无效，更改设定值再寻找）
        search_count = 0
        while search_count < max_searches:
            # 随机设定参数值
            non_lin_vals = [random.uniform(a[0], a[1]) for a in self.init_limits]
            init_coefs = np.array(non_lin_vals)
            # 优化算法计算”最优“参数值
            try:
                tc, m, w, a, b, c1, c2, mse, R2 = self.minimize_lppls(init_coefs, minimizer)
                return tc, m, w, a, b, c1, c2, mse, R2
            # 若无法找到”最优“参数值，search_count+1再次寻找
            except (np.linalg.LinAlgError, UnboundLocalError, ValueError):
                search_count += 1
        # 若n次寻找都无法找到”最优“参数值，返回0值
        return 0, 0, 0, 0, 0, 0, 0, 0, 0


# 计算拉格朗日算子调整的均方差误（用于找出观察样本的最佳起始时间）
def mses1_compute(result, t2):
    # 数据
    temp_t1 = np.linspace(1, t2, t2)
    data_mses = pd.DataFrame({'date': temp_t1[:-30], 'mses': result.mse}).dropna()
    # x, y
    x = data_mses.date.values.reshape(len(data_mses), 1) - t2 + 7
    y = data_mses.mses.values.reshape(len(data_mses), 1)
    # 线性回归
    model = linear_model.LinearRegression()
    fun_l = model.fit(x, y)
    # 拉格朗日算子MSE
    data_mses['mses1'] = data_mses['mses'] - abs(fun_l.coef_[0])*(t2-data_mses['date'])
    return data_mses


# LPPLS计算
def lppls_compute(t, tc, m, w, a, b, c1, c2):
    return a + np.power(tc - t, m) * (b + ((c1 * np.cos(w * np.log(tc - t))) + (c2 * np.sin(w * np.log(tc - t)))))


# 数据读入
def data_in(t1, t2, dir, plot=False):
    # 数据读入
    data_all = pd.read_excel(dir, skipfooter=2)

    # 数据整理
    data = data_all[['日期', '收盘价(元)']].copy()  # 数据切片
    data.columns = ['date', 'close']  # 列重命名
    sta, end = datetime.strptime(t1, '%Y%m%d'), datetime.strptime(t2, '%Y%m%d')  # 起始结束时间
    data_1 = data[(data.date >= sta) & (data.date <= end)]  # 数据截取

    # y,n
    ydata, n, tdata = data_1['close'].values, len(data_1), data_1['date'].values  # 不涉及收益率处理，不做log变换

    # 作图（可选）
    if plot:
        f, ax = plt.subplots(1, 1, figsize=(6, 3))
        ax.plot(data['date'], data['close'])  # 全部数据查看
        ax.plot(data_1['date'], data_1['close'])  # 测算数据查看
        plt.show()

        f, ax1 = plt.subplots(1, 1, figsize=(6, 3))
        xdata = np.linspace(1, n, n)
        ax1.plot(xdata, ydata)  # 测算数据查看
        plt.show()
        return ydata, n, tdata

    return ydata, n, tdata


# 遍历计算（前后都可）
def bl_compute(t1, t2, max_a, process_num, compute_num, mode, bl, file_dir1, file_dir2, file_dir3):
    # 数据读入
    ydata, n, tdata = data_in(t1, t2, file_dir1)
    # 遍历设定
    if mode == 't1':
        sta_range = np.arange(0, n-30, bl)
    else:
        sta_range = np.arange(30, n, bl)

    # 遍历t1，估计LPPLS模型
    result_trav = []
    for sta in sta_range:
        print(sta)

        # 数据
        if mode == 't1':
            y = ydata[sta:]
            startday, endday = tdata[sta], tdata[-1]
        else:
            y = ydata[:sta]
            startday, endday = tdata[0], tdata[sta]
        x = np.linspace(1, len(y), len(y))
        observations = np.array([x, y])

        # 算法设定
        model = LPPLS(observations=observations)
        max_searches, minimiza_method = 100, 'SLSQP'

        # 多进程计算
        pool = multiprocessing.Pool(processes=process_num)
        # 每个时间点计算n次
        results = []
        for pool_i in range(compute_num):
            results.append(pool.apply_async(model.fit, (max_searches, minimiza_method)))
        pool.close()
        pool.join()
        print("multiprocessing done.")

        # n次禁忌搜索结果整理
        tc_, m_, w_, a_, b_, c1_, c2_, mse_, R2_ = [], [], [], [], [], [], [], [], []
        for i in range(len(results)):
            data = results[i].get()
            tc_.append(data[0])
            m_.append(data[1])
            w_.append(data[2])
            a_.append(data[3])
            b_.append(data[4])
            c1_.append(data[5])
            c2_.append(data[6])
            mse_.append(data[7])
            R2_.append(data[8])
        temp_result = pd.DataFrame({'tc': tc_, 'm': m_, 'w': w_, 'a': a_, 'b': b_, 'c1': c1_, 'c2': c2_, 'mse': mse_, 'R2': R2_})

        # 参数估计结果筛选
        temp_result_ = temp_result[(temp_result.a < max_a) & (temp_result.b < 0)].copy()
        temp_result_.sort_values('mse', ascending=True, inplace=True)
        time_data = pd.Series([len(y), startday, endday], index=['len', 'startday', 'endday'])
        try:
            model_result = temp_result_.iloc[0, :].copy()
            result_trav.append(time_data.append(model_result))
        except:
            model_result = pd.Series([0] * 9, index=['tc', 'm', 'w', 'a', 'b', 'c1', 'c2', 'mse', 'R2'])
            result_trav.append(time_data.append(model_result))

    # 参数估计结果汇总
    result = pd.concat(result_trav, axis=1)
    result = result.T
    result['breakday_predict'] = result['tc'] - result['len']

    if bl == 1:
        # 2个结果输出
        result.to_excel(file_dir2, index=False)
        # 计算拉格朗日算子调整mses
        data_mses = mses1_compute(result, n)
        data_mses.to_excel(file_dir3, index=False)
    else:
        # 1个结果输出
        result.to_excel(file_dir2, index=False)


# 固定时间点计算
def fix_compute(t1, t2, max_a, process_num, compute_num, file_dir1, file_dir2):
    # 数据读入
    ydata, n, tdata = data_in(t1, t2, file_dir1)

    # 数据整理
    y = ydata.copy()
    x = np.linspace(1, n, n)
    observations = np.array([x, y])

    # 估计算法设定
    model = LPPLS(observations=observations)
    max_searches, minimiza_method = 100, 'SLSQP'

    # 多进程计算
    pool = multiprocessing.Pool(processes=process_num)
    results = []
    for pool_i in range(compute_num):
        results.append(pool.apply_async(model.fit, (max_searches, minimiza_method)))
    pool.close()
    pool.join()
    print("multiprocessing done.")

    # n次禁忌搜索结果整理
    tc_, m_, w_, a_, b_, c1_, c2_, mse_, R2_ = [], [], [], [], [], [], [], [], []
    for j in range(len(results)):
        data = results[j].get()
        tc_.append(data[0])
        m_.append(data[1])
        w_.append(data[2])
        a_.append(data[3])
        b_.append(data[4])
        c1_.append(data[5])
        c2_.append(data[6])
        mse_.append(data[7])
        R2_.append(data[8])
    temp_result = pd.DataFrame({'tc': tc_, 'm': m_, 'w': w_, 'a': a_, 'b': b_, 'c1': c1_, 'c2': c2_, 'mse': mse_, 'R2': R2_})
    # 筛选
    result = temp_result[(temp_result.a < max_a) & (temp_result.b < 0)].copy()
    result.sort_values('mse', ascending=True, inplace=True)
    # break
    result['breakday_predict'] = result['tc'] - len(y)
    # 结果输出
    result.to_excel(file_dir2, index=False)

