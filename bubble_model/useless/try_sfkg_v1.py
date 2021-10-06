import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing
from bubble_model.useless.LPPLS_WYQ_old import LPPLS, data_in, mses1_compute


if __name__ == '__main__':
    t = datetime.now()

    # 数据准备
    mulp = 2
    result_num = 150
    t1, t2, date_x, file_dir = '20181001', '20201231', 30, r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\sfkg.xls'
    ydata, n = data_in(t1, t2, file_dir)
    file_name1, file_name2 = 'resuLt_sfkg.xls', 'mse1_sfkg.xls'

    # 遍历t1（1，n-30)，估计LPPLS模型
    result_trav = []
    for sta in range(n - 30):
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
        # 每个时间点计算n次
        for pool_i in range(result_num):
            results.append(pool.apply_async(model.fit, (max_searches, minimiza_method)))
        pool.close()
        pool.join()
        print("multiprocessing done.")

        # n次禁忌搜索结果整理
        tc_, m_, w_, a_, b_, c1_, c2_, mse_ = [], [], [], [], [], [], [], []
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
        temp_result = pd.DataFrame({'tc': tc_, 'm': m_, 'w': w_, 'a': a_, 'b': b_, 'c1': c1_, 'c2': c2_, 'mse': mse_})

        # 参数估计结果筛选
        temp_result_ = temp_result[(temp_result.a < max(y)+3) & (temp_result.b < 0)].copy()
        temp_result_.sort_values('mse', ascending=True, inplace=True)
        time_data = pd.Series([sta, len(y) + date_x], index=['startday', 'breakday'])
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

    # 计算拉格朗日算子调整mses
    data_mses = mses1_compute(result, n)

    # 数据输出
    result.to_excel('C:\\Users\\Administrator\\Desktop\\' + file_name1, index=False)
    data_mses.to_excel('C:\\Users\\Administrator\\Desktop\\' + file_name2, index=False)

    print(datetime.now() - t)

