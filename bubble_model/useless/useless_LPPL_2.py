import pandas as pd
import numpy as np
from datetime import datetime
from matplotlib import pyplot as plt
import random
from scipy.optimize import minimize
from sklearn import linear_model
import multiprocessing


class LPPLS(object):

    def __init__(self, observations):
        self.observations = observations

    def lppls(self, t, tc, m, w, a, b, c1, c2):
        return a + np.power(tc - t, m) * (b + ((c1 * np.cos(w * np.log(tc - t))) + (c2 * np.sin(w * np.log(tc - t)))))

    def matrix_equation(self, observations, tc, m, w):
        T = observations[0]
        P = observations[1]

        deltaT = tc - T
        phase = np.log(deltaT)
        fi = np.power(deltaT, m)
        gi = fi * np.cos(w * phase)
        hi = fi * np.sin(w * phase)
        A = np.stack((np.ones_like(deltaT), fi, gi, hi))

        return np.linalg.lstsq(A.T, P, rcond=-1.0)[0]

    def mse(self, x, *args):
        tc = x[0]
        m = x[1]
        w = x[2]
        obs = args[0]

        a, b, c1, c2 = self.matrix_equation(obs, tc, m, w)

        delta = [self.lppls(t, tc, m, w, a, b, c1, c2) for t in obs[0, :]]
        delta = np.subtract(delta, obs[1, :])
        delta = np.power(delta, 2)

        return np.sum(delta) / (len(obs)+7)

    def minimize_(self, seed, minimizer):
        cofs = minimize(fun=self.mse, x0=seed, args=self.observations, method=minimizer, bounds=self.init_limits)

        if cofs.success:
            tc = cofs.x[0]
            m = cofs.x[1]
            w = cofs.x[2]

            a, b, c1, c2 = self.matrix_equation(self.observations, tc, m, w)

            delta = [self.lppls(t, tc, m, w, a, b, c1, c2) for t in self.observations[0, :]]
            delta = np.subtract(delta, self.observations[1, :])
            delta = np.power(delta, 2)
            mse = np.sum(delta) / (len(self.observations) + 7)

            return tc, m, w, a, b, c1, c2, mse
        else:
            raise UnboundLocalError

    def fit(self, max_searches=25, minimizer='Nelder-Mead'):
        search_count = 0
        # find bubble
        while search_count < max_searches:
            # set random initialization limits for non-linear params
            t_first, t_last = self.observations[0, 0], self.observations[0, -1]
            tc_init_min, tc_init_max = t_last, t_last * 2
            # t_delta = t_last - t_first
            # tc_init_min, tc_init_max = t_last - t_delta, t_last + t_delta

            self.init_limits = [
                (tc_init_min, tc_init_max),  # tc : Critical Time
                (0, 1),  # m : 0.1 ≤ m ≤ 0.9
                (2, 17),  # ω : 6 ≤ ω ≤ 13
            ]

            # randomly choose vals within bounds for non-linear params
            non_lin_vals = [random.uniform(a[0], a[1]) for a in self.init_limits]
            seed = np.array(non_lin_vals)

            # Increment search count on SVD convergence error, but raise all other exceptions.
            try:
                tc, m, w, a, b, c1, c2, mse = self.minimize_(seed, minimizer)
                return tc, m, w, a, b, c1, c2, mse
            except (np.linalg.LinAlgError, UnboundLocalError, ValueError):
                search_count += 1
        return 0, 0, 0, 0, 0, 0, 0, 0

# 数据读入
def data_in(t1, t2, plot=False):
    data_all = pd.read_excel('C:\\Users\\Administrator\\Desktop\\' + 'wind.xls')

    # 数据整理
    data = data_all[['日期', '收盘价(元)']].copy() # 数据切片
    data = data.drop(data.tail(3).index)  # 去除na
    data.columns = ['date', 'close']  # 列重命名

    # 数据截取
    sta = datetime.strptime(t1, '%Y%m%d')  # 起始结束时间
    end = datetime.strptime(t2, '%Y%m%d')
    data_1 = data[(data.date > sta) & (data.date < end)]

    # 日期转为序号，设置x,y
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
    return ydata, n


def myplot(x, y):
    f, ax = plt.subplots(1, 1, figsize=(6, 3))
    ax.plot(x, y, 'r-')
    plt.show()


if __name__ == '__main__':
    t = datetime.now()
    # 数据准备
    ydata, n = data_in('20130101', '20150501')
    date_x = 30
    # 遍历，并计算mses
    result_trav = []
    # brds, tcs, ms, ws, a_s, bs, c1s, c2s, mses, sta_dates = [], [], [], [], [], [], [], [], [], []

    for sta in range(n - 30):
    # for sta in [-50, -100, -200, -300, -400, -500]:
        print(sta)
        y = ydata[sta:]
        x = np.linspace(0, len(y) - 1, len(y))
        observations = np.array([x, y])
        model = LPPLS(observations=observations)
        max_searches, minimiza_method = 100, 'SLSQP'

        pool = multiprocessing.Pool(processes=4)
        results = []
        for pool_i in range(50):
            results.append(pool.apply_async(model.fit, (max_searches, minimiza_method)))
        pool.close()
        pool.join()
        print("Sub-process(es) done.")
        tc_, m_, w_, a_, b_, c1_, c2_, mse_ = [], [], [], [], [], [], [], []
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
        temp_result = pd.DataFrame({'tc': tc_, 'm': m_, 'w': w_, 'a': a_, 'b': b_, 'c1': c1_, 'c2': c2_, 'mse': mse_})
        temp_result_ = temp_result[(temp_result.a < 14) & (temp_result.b < 0)].copy()
        temp_result_.sort_values('mse', ascending=True, inplace=True)
        try:
            temp2 = temp_result_.iloc[0, :].copy()
            temp1 = pd.Series([sta, len(y) + date_x], index=['startday', 'breakday'])
            # temp1 = pd.Series([len(ydata) + sta, len(y) + date_x], index=['startday', 'breakday'])
            result_trav.append(temp1.append(temp2))
        except:
            temp3 = pd.Series([0] * 10, index=['startday', 'breakday', 'tc', 'm', 'w', 'a', 'b', 'c1', 'c2', 'mse'])
            result_trav.append(temp3)
            continue
    result = pd.concat(result_trav, axis=1)
    result = result.T
        # for j in range(len(result)):
        #     if j == 0:
        #         data = result[j].get()
        #         mse = data[-1]
        #         tc, m, w, a, b, c1, c2 = data[0], data[1], data[2], data[3], data[4], data[5], data[6]
        #     else:
        #         data = result[j].get()
        #         if mse > data[-1]:
        #             mse = data[-1]
        #             tc, m, w, a, b, c1, c2 = data[0], data[1], data[2], data[3], data[4], data[5], data[6]
        #         else:
        #             continue
        # brds.append(len(x) + 30)
        # tcs.append(tc)
        # ms.append(m)
        # ws.append(w)
        # a_s.append(a)
        # bs.append(b)
        # c1s.append(c1)
        # c2s.append(c2)
        # mses.append(mse)
        # sta_dates.append(sta)
    # result = pd.DataFrame({'breakday': brds, 'tc': tcs,
    #                        'm': ms, 'w': ws, 'a': a_s, 'b': bs, 'c1': c1s, 'c2': c2s, 'mse': mses,
    #                        'stadate': sta_dates})

    # 计算mses1
    fun_reg = linear_model.LinearRegression()
    xdata = np.linspace(0, n-1, n)
    data_mses = pd.DataFrame({'date': xdata[:-30]-n+7, 'mses': result.mse}).dropna()
    x, y = data_mses.date.values.reshape(len(data_mses), 1), data_mses.mses.values.reshape(len(data_mses), 1)
    fun_l = fun_reg.fit(x, y)
    data_mses['mses1'] = data_mses['mses'] + fun_l.coef_[0]*(-data_mses['date'])

    # 数据输出
    result.to_excel('C:\\Users\\Administrator\\Desktop\\' + 'result.xls', index=False)
    data_mses.to_excel('C:\\Users\\Administrator\\Desktop\\' + 'mses1.xls', index=False)
    print(datetime.now() - t)

