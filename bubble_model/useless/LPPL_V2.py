import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing
from bubble_model.useless.LPPLS_WYQ_old import LPPLS, data_in


if __name__ == '__main__':
    t = datetime.now()

    # 数据准备
    mulp = 1
    result_num = 150
    dir = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\wind.xls'
    # ydata, n = data_in('20130101', '20150301', dir)
    # month, date_x = '_3', 75
    # ydata, n = data_in('20130101', '20150401', dir)
    # month, date_x = '_4', 53
    # ydata, n = data_in('20130101', '20150501', dir)
    # month, date_x = '_5', 32
    ydata, n = data_in('20130101', '20150601', dir)
    month, date_x = '_6', 12
    file_name = 'result' + month + '.xls'

    # # 遍历t1（限定）估计LPPLS模型
    result_trav = []
    for sta in np.arange(-50, -len(ydata), -25):
        print(sta)

        # 数据切片
        y = ydata[sta:]
        x = np.linspace(1, len(y), len(y))
        observations = np.array([x, y])

        # 估计算法设定
        model = LPPLS(observations=observations)
        max_searches, minimiza_method = 100, 'SLSQP'

        # 多进程计算
        pool = multiprocessing.Pool(processes=mulp)
        results = []
        for pool_i in range(result_num):
            results.append(pool.apply_async(model.fit, (max_searches, minimiza_method)))
        pool.close()
        pool.join()
        print("Sub-process(es) done.")

        # n次禁忌搜索结果整理
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

        # 参数估计结果筛选
        temp_result_ = temp_result[(temp_result.a < max(y)+3) & (temp_result.b < 0)].copy()
        temp_result_.sort_values('mse', ascending=True, inplace=True)
        time_data = pd.Series([len(y), len(y) + date_x], index=['len', 'breakday'])
        try:
            model_result = temp_result_.iloc[0, :].copy()
            result_trav.append(time_data.append(model_result))
        except:
            model_result = pd.Series([0] * 8, index=['tc', 'm', 'w', 'a', 'b', 'c1', 'c2', 'mse'])
            result_trav.append(time_data.append(model_result))

    # 参数估计结果汇总
    result = pd.concat(result_trav, axis=1)
    result = result.T
    result['error'] = result['breakday'] - result['tc']

    # 数据输出
    result.to_excel('C:\\Users\\Administrator\\Desktop\\' + file_name, index=False)

    print(datetime.now() - t)

