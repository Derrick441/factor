import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing
from bubble_model.useless.LPPLS_WYQ_old import LPPLS, data_in


def compute(mulp, result_num, ydata, file_name):
    # 遍历t1（限定）估计LPPLS模型
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
        time_data = pd.Series([len(y)], index=['len'])
        try:
            model_result = temp_result_.iloc[0, :].copy()
            result_trav.append(time_data.append(model_result))
        except:
            model_result = pd.Series([0] * 8, index=['tc', 'm', 'w', 'a', 'b', 'c1', 'c2', 'mse'])
            result_trav.append(time_data.append(model_result))

    # 参数估计结果汇总
    result = pd.concat(result_trav, axis=1)
    result = result.T
    result['breakday_predict'] = result['tc'] - result['len']

    # 数据输出
    result.to_excel('C:\\Users\\Administrator\\Desktop\\' + file_name, index=False)


# 3个股票
if __name__ == '__main__':
    t = datetime.now()

    # 数据准备
    mulp = 2
    result_num = 150

    ydata1, n1 = data_in('20200101', '20210603', r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\奥园美谷复权数据.xls')
    file_name1 = 'result_奥园美谷.xls'

    ydata2, n2 = data_in('20200101', '20210603', r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\国际医学复权数据.xls')
    file_name2 = 'result_国际医学.xls'

    ydata3, n3 = data_in('20180701', '20210603', r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\东方雨虹复权数据.xls')
    file_name3 = 'result_东方雨虹.xls'

    compute(mulp, result_num, ydata1, file_name1)
    compute(mulp, result_num, ydata2, file_name2)
    compute(mulp, result_num, ydata3, file_name3)

    print(datetime.now() - t)

