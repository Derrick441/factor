import pandas as pd
import numpy as np
from datetime import datetime
import random
from scipy.optimize import minimize
import multiprocessing
import cx_Oracle
from matplotlib import pyplot as plt


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
def bl_compute(data_get, stock, t1, t2, keep_num, max_time, process_num, compute_num, mode, bl, ln, file_dir):
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
    if ln == 1:
        price_list = np.log(price_list)
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

