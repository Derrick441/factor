import pandas as pd
import numpy as np
from datetime import datetime
import random
from scipy.optimize import minimize
import multiprocessing
import cx_Oracle
from matplotlib import pyplot as plt


# LPPLS模型：禁忌搜索+SLSQP
class LPPLS(object):

    def __init__(self, observations):
        # 传入观察数据（x,y）
        self.observations = observations
        self.T = observations[0]
        self.P = observations[1]
        # 设定参数约束条件（3个）
        self.init_limits = [
            (self.T[-1]+0.01, self.T[-1]+250),  # tc : 崩盘时间暂设定为截止时间后0.01-250天
            (0.01, 0.99),  # m : 0.1 ≤ m ≤ 0.9
            (3, 15)  # ω : 6 ≤ ω ≤ 13
        ]

    # lppls公式
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
        # 非线性参数和观察数据
        tc, m, w, obs = x[0], x[1], x[2], args[0]
        # 线性参数
        a, b, c1, c2 = self.matrix_equation(obs, tc, m, w)
        # 预测值
        y_ = [self.lppls(t, tc, m, w, a, b, c1, c2) for t in obs[0, :]]
        # 均方差误
        delta = np.subtract(y_, obs[1, :])
        delta_s = np.power(delta, 2)
        result_mse = np.sum(delta_s) / (len(obs)+7)
        return result_mse

    def minimize_lppls(self, init_coefs, minimizer):
        # 使用scipy的minimize计算出最小化mse的非线性参数
        cofs = minimize(fun=self.mse, x0=init_coefs, args=self.observations, method=minimizer, bounds=self.init_limits)
        # 计算非线性参数和mse、R2
        if cofs.success:
            # 7个参数
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
            # 返回9个值
            return tc, m, w, a, b, c1, c2, mse, R2
        else:
            raise UnboundLocalError

    def fit(self, max_searches=100, minimizer='SLSQP'):
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
            except(np.linalg.LinAlgError, UnboundLocalError, ValueError):
                search_count += 1
        # 若n次寻找都无法找到”最优“参数值，返回0值
        return 0, 0, 0, 0, 0, 0, 0, 0, 0


# LPPLS计算
def lppls_compute(t, tc, m, w, a, b, c1, c2):
    return a + np.power(tc - t, m) * (b + ((c1 * np.cos(w * np.log(tc - t))) + (c2 * np.sin(w * np.log(tc - t)))))


# sql读入数据
def data_sql_readin(account, password, ip_db, columns, column_names, table, key1, stock, key2, t1, t2):
    # 数据读入
    db = cx_Oracle.connect(account, password, ip_db)
    sql = '''
    select {0}
    from {1}
    where {2}='{3}'
    and {4}>='{5}'
    and {4}<='{6}'
    '''.format(columns, table, key1, stock, key2, t1, t2)
    data = pd.read_sql(sql, con=db)
    # 数据整理
    data.columns = column_names
    data.sort_values(by=['date'], inplace=True)
    data.reset_index(drop=True, inplace=True)
    return data


# excel读入数据
def data_excel_readin(excel_dir, excel_columns, excel_column_names, t1, t2):
    # 数据读入
    data_all = pd.read_excel(excel_dir, skipfooter=2, engine='openpyxl')
    # 数据整理
    data = data_all[excel_columns].copy()  # 数据切片
    data.columns = excel_column_names  # 列重命名
    # 数据截取
    sta = datetime.strptime(t1, '%Y%m%d')
    end = datetime.strptime(t2, '%Y%m%d')
    result = data[(data.date >= sta) & (data.date <= end)]
    return result


# 每日多进程计算
def multi_compute(process_num, compute_num, model, max_searches, minimiza_method):
    # 每个时间点计算process_num次
    results = []
    pool = multiprocessing.Pool(processes=process_num)
    for pool_i in range(compute_num):
        results.append(pool.apply_async(model.fit, (max_searches, minimiza_method)))
    pool.close()
    pool.join()
    # process_num次计算结果整理
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
    return pd.DataFrame({'tc': tc_, 'm': m_, 'w': w_, 'a': a_, 'b': b_, 'c1': c1_, 'c2': c2_, 'mse': mse_, 'R2': R2_})


# 遍历计算（前后都可）
def bl_compute(data_get, stock, t1, t2, keep_num, max_time, process_num, compute_num, mode, bl, file_dir):
    # SQL参数
    account = 'wind_read'
    password = 'password1'
    ip_db = '84.238.189.43/itsuat'
    columns = 's_info_windcode,trade_dt,s_dq_close*s_dq_adjfactor close_adj'
    column_names = ['code', 'date', 'close_adj']
    table = 'wind.AShareEODPrices'
    key1 = 's_info_windcode'
    key2 = 'trade_dt'
    # excel参数
    excel_dir = file_dir + stock[:6] + '.xlsx'
    excel_columns = ['代码', '日期', '收盘价(元)']
    excel_column_names = ['code', 'date', 'close_adj']

    # 读入数据
    if data_get == 'sql':
        data_read = data_sql_readin(account, password, ip_db, columns, column_names, table, key1, stock, key2, t1, t2)
    else:
        data_read = data_excel_readin(excel_dir, excel_columns, excel_column_names, t1, t2)
    price_list = data_read.close_adj.to_list()
    date_list = data_read.date.to_list()

    # 遍历设定
    data_num = len(data_read)
    if mode == 't1':
        sta_range = np.arange(0, data_num-keep_num, bl)
    else:
        sta_range = np.arange(keep_num, data_num, bl)

    # 遍历时间，估计LPPLS模型
    result_trav = []
    for sta in sta_range:
        t = datetime.now()
        # 数据整理
        if mode == 't1':
            y = price_list[sta:]
            startday = date_list[sta]
            endday = date_list[-1]
        else:
            y = price_list[:sta]
            startday = date_list[0]
            endday = date_list[sta]
        x = np.linspace(1, len(y), len(y))
        observations = np.array([x, y])
        # 算法设定
        model = LPPLS(observations=observations)
        max_searches = 100
        minimiza_method = 'SLSQP'
        # 多进程计算
        temp_result = multi_compute(process_num, compute_num, model, max_searches, minimiza_method)
        # 结果筛选
        condition1 = temp_result.a < (max(y) * max_time)
        condition2 = temp_result.b < 0
        temp_result_ = temp_result[condition1 & condition2].copy()
        # 保留最好的
        mode_coefs = temp_result_.sort_values('mse', ascending=True)
        data_lens = pd.Series([len(y), startday, endday], index=['len', 'startday', 'endday'])
        try:
            model_result = mode_coefs.iloc[0, :].copy()
            result_trav.append(data_lens.append(model_result))
        except:
            model_result = pd.Series([np.nan] * 9, index=['tc', 'm', 'w', 'a', 'b', 'c1', 'c2', 'mse', 'R2'])
            result_trav.append(data_lens.append(model_result))
        print(sta, " multiprocessing done.", datetime.now() - t)
    # 参数估计结果汇总
    result = pd.concat(result_trav, axis=1)
    result = result.T
    # 崩盘日
    result['breakday_predict'] = result['tc'] - result['len']
    result['startday'] = result['startday'].apply(lambda x: datetime.strftime(x, '%Y%m%d'))
    result['endday'] = result['endday'].apply(lambda x: datetime.strftime(x, '%Y%m%d'))
    # 结果输出
    dir_part = '_' + mode + '_' + t1 + '_' + t2
    file_dir_ = file_dir + stock[:6] + dir_part + '.xlsx'
    result.to_excel(file_dir_, index=False, float_format='%.4f')


# 固定时间点计算
def fix_compute(data_get, stock, t1, t2, process_num, compute_num, file_dir):
    # SQL参数
    account = 'wind_read'
    password = 'password1'
    ip_db = '84.238.189.43/itsuat'
    columns = 's_info_windcode,trade_dt,s_dq_close*s_dq_adjfactor close_adj'
    column_names = ['code', 'date', 'close_adj']
    table = 'wind.AShareEODPrices'
    key1 = 's_info_windcode'
    key2 = 'trade_dt'
    # excel参数
    excel_dir = file_dir + stock[:6] + '.xlsx'
    excel_columns = ['代码', '日期', '收盘价(元)']
    excel_column_names = ['code', 'date', 'close_adj']

    # 读入数据
    if data_get == 'sql':
        data_read = data_sql_readin(account, password, ip_db, columns, column_names, table, key1, stock, key2, t1, t2)
    else:
        data_read = data_excel_readin(excel_dir, excel_columns, excel_column_names, t1, t2)

    # 数据整理
    y = data_read.close_adj.to_list()
    x = np.linspace(1, len(y), len(y))
    observations = np.array([x, y])

    # 估计算法设定
    model = LPPLS(observations=observations)
    max_searches, minimiza_method = 100, 'SLSQP'

    # 多进程计算
    t = datetime.now()
    pool = multiprocessing.Pool(processes=process_num)
    results = []
    for pool_i in range(compute_num):
        results.append(pool.apply_async(model.fit, (max_searches, minimiza_method)))
    pool.close()
    pool.join()
    print("multiprocessing done.", datetime.now() - t)

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

    # 结果筛选
    condition1 = temp_result.a < (max(y) * 3)
    condition2 = temp_result.b < 0
    temp_result_ = temp_result[condition1 & condition2].copy()
    # 排序
    result = temp_result_.sort_values('mse', ascending=True)

    # break
    result['breakday_predict'] = result['tc'] - len(y)
    # 结果输出
    dir_part = '_' + str(compute_num) + '_' + t1 + '_' + t2
    file_dir_ = file_dir + stock[:6] + dir_part + '.xlsx'
    result.to_excel(file_dir_, index=False, float_format='%.4f')


# 每日for计算
def for_compute(max_time, mode, sta, price_list, date_list, compute_num):
    t = datetime.now()
    # 数据整理
    if mode == 't1':
        y = price_list[sta:]
        startday = date_list[sta]
        endday = date_list[-1]
    else:
        y = price_list[:sta]
        startday = date_list[0]
        endday = date_list[sta]
    x = np.linspace(1, len(y), len(y))
    observations = np.array([x, y])
    # 算法设定
    model = LPPLS(observations=observations)
    max_searches = 100
    minimiza_method = 'SLSQP'
    # for计算
    results = []
    for i in range(compute_num):
        results.append(model.fit(max_searches, minimiza_method))
    # process_num次计算结果整理
    tc_, m_, w_, a_, b_, c1_, c2_, mse_, R2_ = [], [], [], [], [], [], [], [], []
    for i in range(len(results)):
        data = results[i]
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
    # 结果筛选
    condition1 = temp_result.a < (max(y) * max_time)
    condition2 = temp_result.b < 0
    temp_result_ = temp_result[condition1 & condition2].copy()
    # 保留最好的
    mode_coefs = temp_result_.sort_values('mse', ascending=True)
    data_lens = pd.Series([len(y), startday, endday], index=['len', 'startday', 'endday'])
    print(sta, datetime.now() - t)
    try:
        model_result = mode_coefs.iloc[0, :].copy()
        return data_lens.append(model_result)
    except:
        model_result = pd.Series([np.nan] * 9, index=['tc', 'm', 'w', 'a', 'b', 'c1', 'c2', 'mse', 'R2'])
        return data_lens.append(model_result)


# 遍历计算（前后都可）多进程
def bl_compute_new(data_get, stock, t1, t2, keep_num, max_time, process_num, compute_num, mode, bl, file_dir):
    # SQL参数
    account = 'wind_read'
    password = 'password1'
    ip_db = '84.238.189.43/itsuat'
    columns = 's_info_windcode,trade_dt,s_dq_close*s_dq_adjfactor close_adj'
    column_names = ['code', 'date', 'close_adj']
    table = 'wind.AShareEODPrices'
    key1 = 's_info_windcode'
    key2 = 'trade_dt'
    # excel参数
    excel_dir = file_dir + stock[:6] + '.xlsx'
    excel_columns = ['代码', '日期', '收盘价(元)']
    excel_column_names = ['code', 'date', 'close_adj']

    # 读入数据
    if data_get == 'sql':
        data_read = data_sql_readin(account, password, ip_db, columns, column_names, table, key1, stock, key2, t1, t2)
    else:
        data_read = data_excel_readin(excel_dir, excel_columns, excel_column_names, t1, t2)
    price_list = data_read.close_adj.to_list()
    date_list = data_read.date.to_list()

    # 遍历设定
    data_num = len(data_read)
    if mode == 't1':
        sta_range = np.arange(0, data_num-keep_num, bl)
    else:
        sta_range = np.arange(keep_num, data_num, bl)

    # 多进程计算
    result_trav = []
    pool = multiprocessing.Pool(processes=process_num)
    for sta in sta_range:
        result_trav.append(pool.apply_async(for_compute, (max_time, mode, sta, price_list, date_list, compute_num)))
    pool.close()
    pool.join()
    # 参数估计结果汇总
    result_trav_ = []
    for i in range(len(result_trav)):
        result_trav_.append(result_trav[i].get())
    result = pd.concat(result_trav_, axis=1)
    result = result.T

    # 崩盘日
    result['breakday_predict'] = result['tc'] - result['len']
    result['startday'] = result['startday'].apply(lambda x: datetime.strftime(x, '%Y%m%d'))
    result['endday'] = result['endday'].apply(lambda x: datetime.strftime(x, '%Y%m%d'))
    if mode == 't2':
        result['price'] = np.array(price_list)[sta_range]
    # 结果输出
    dir_part = '_' + mode + '_' + t1 + '_' + t2
    file_dir_ = file_dir + stock[:6] + dir_part + '.xlsx'
    result.to_excel(file_dir_, index=False, float_format='%.4f')


def lppls_picture(stock, t1, t2, result_dir, mode, compute_num):
    # 数据读入
    if mode in ('t1', 't2'):
        dir_part = '_' + mode + '_' + t1 + '_' + t2
        result_dir_ = result_dir + stock[:6] + dir_part + '.xlsx'
    else:
        dir_part = '_' + str(compute_num) + '_' + t1 + '_' + t2
        result_dir_ = result_dir + stock[:6] + dir_part + '.xlsx'
    result_all = pd.read_excel(result_dir_, engine='openpyxl')

    # 图片保存地址
    picture_dir = result_dir + stock[:6] + dir_part + '_'
    # 作图1
    if mode in ('t1', 't2'):
        y1 = result_all.breakday_predict
        y2 = result_all.R2
        x = np.linspace(1, len(y1), len(y1))
        # 双折线图(breakday_predict,R2)
        f, ax1 = plt.subplots(1, 1, figsize=(8, 5))
        l1 = ax1.plot(x, y1, c='b', ls='-', label='breakday_predict')
        ax2 = ax1.twinx()
        l2 = ax2.plot(x, y2, c='r', ls='-', label='R2')
        plt.legend(l1 + l2, ["breakday_predict", "R2"])
        plt.savefig(picture_dir + 'fig1_breakday_predict&R2_lines.png')
        # plt.show()
    else:
        y1 = result_all.breakday_predict
        # 密度分布图(breakday_predict)
        f1, ax1 = plt.subplots(1, 1, figsize=(8, 5))
        ax1.hist(y1, bins=100, label='breakday_predict')
        ax1.legend(loc=1)
        plt.savefig(picture_dir + 'fig1_breakday_predict_hist.png')
        # plt.show()

    # 作图2
    if mode == 't1':
        # 密度分布图(breakday_predict)
        f3, ax3 = plt.subplots(1, 1, figsize=(8, 5))
        ax3.hist(y1, bins=100, label='breakday_predict')
        ax3.legend(loc=1)
        plt.savefig(picture_dir + 'fig2_breakday_predict_hist.png')
        # plt.show()
    elif mode == 't2':
        x = np.linspace(1, len(y1), len(y1))
        y3 = result_all.price
        # 双折线图（breakday_predict, price)
        f3, ax3 = plt.subplots(1, 1, figsize=(8, 5))
        l3 = ax3.plot(x, y3, c='b', ls='-', label='price')
        ax4 = ax3.twinx()
        l4 = ax4.plot(x, y1, c='r', ls='-', label='breakday_predict')
        plt.legend(l3 + l4, ["price", "breakday_predict"])
        plt.savefig(picture_dir + 'fig2_breakday_predict&price_lines.png')
        # plt.show()

        # 多线重叠图(price)
        result_all['price_1'] = result_all[result_all['breakday_predict'] < 2]['price']
        result_all['price_2'] = result_all[result_all['breakday_predict'] >= 2]['price']
        # 作图展示
        f, ax = plt.subplots(1, 1, figsize=(8, 5))
        ax.plot(x, result_all.price, c='b', ls='-', label='price')
        ax.plot(x, result_all.price_1, c='r', ls='-', label='dangerous')
        ax.plot(x, result_all.price_2, c='g', ls='-', label='safe600')
        ax.legend(loc=1)
        plt.savefig(picture_dir + 'fig3_prices_lines.png')
        # plt.show()

