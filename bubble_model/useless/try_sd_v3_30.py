import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing
from bubble_model.useless.LPPLS_WYQ_old import LPPLS, data_in

if __name__ == '__main__':
    t = datetime.now()

    # 数据准备
    mulp = 3
    result_num = 3000
    dir = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\舍得酒业半小时复权数据.xls'
    ydata, n = data_in('20210309', '20210604', dir)
    file_name = 'result_sd舍得_30.xls'

    # 数据切片
    y = ydata.copy()
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
    # 筛选
    result = temp_result[(temp_result.a < max(y)+3) & (temp_result.b < 0)].copy()
    result.sort_values('mse', ascending=True, inplace=True)

    result['breakday_predict'] = result['tc'] - len(y)

    result.to_excel('C:\\Users\\Administrator\\Desktop\\' + file_name, index=False)

    print(datetime.now() - t)

    # import pandas as pd
    # look = pd.read_excel('C:\\Users\\Administrator\\Desktop\\' + 'result_sd舍得_30.xls')
    # look.breakday_predict.hist(bins=100)
    #
    # dir = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\舍得酒业半小时复权数据.xls'
    # ydata, n = data_in('20210309', '20210604', dir)
    #
    # x1 = np.arange(1, n + 1, 1)
    # x2 = np.arange(1, n + 8*60, 1)
    #
    # coef = look.iloc[0, :]
    # ydata_predict = lppls_compute(x2, coef[0], coef[1], coef[2], coef[3], coef[4], coef[5], coef[6])
    #
    # f, ax = plt.subplots(1, 1, figsize=(6, 3))
    # ax.plot(x1, ydata, c='b', ls='-')
    # ax.plot(x2, ydata_predict, c='r', ls='--')
    # ax.vlines(x=coef[0], ymin=min(ydata), ymax=coef[3] + 0.1, colors='#FFA500', linestyles='--')
    # plt.show()
