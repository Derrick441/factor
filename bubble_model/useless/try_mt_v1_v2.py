from datetime import datetime
from bubble_model.useless.LPPLS_WYQ_old import bl_compute

if __name__ == '__main__':
    # 常规设定
    process_num = 3  # 进程数
    compute_num = 100  # 单个时间计算数量
    file_dir1 = r'C:\Users\Administrator\Desktop\LPPL模型\数据资料\贵州茅台复权数据.xls'  # 数据地址
    max_a = 8000  # 结果筛选设定

    # 变动设定
    t1 = '20181031'  # 全部数据起始时间
    t2 = '20210610'  # 全部数据结束时间
    mode = 't1'  # 遍历模式（t1:起始时间遍历，t2:结束时间遍历）
    bl = 1  # 遍历间隔天数
    file_dir2 = 'C:\\Users\\Administrator\\Desktop\\result_mt_' + mode + '.xls'  # 结果保存地址
    file_dir3 = 'C:\\Users\\Administrator\\Desktop\\mses1_mt_' + mode + '.xls'

    # 遍历计算
    t = datetime.now()
    bl_compute(process_num, compute_num, t1, t2, file_dir1, mode, bl, max_a, file_dir2, file_dir3)
    print(mode, datetime.now() - t)

    # 变动设定
    t1 = '20181031'
    t2 = '20210610'
    mode = 't2'
    bl = 1
    file_dir2 = 'C:\\Users\\Administrator\\Desktop\\result_mt_' + mode + '.xls'
    file_dir3 = 'C:\\Users\\Administrator\\Desktop\\mses1_mt_' + mode + '.xls'

    # 遍历计算
    t = datetime.now()
    bl_compute(process_num, compute_num, t1, t2, file_dir1, mode, bl, max_a, file_dir2, file_dir3)
    print(mode, datetime.now() - t)

    # # 查看 #################################################################################
    # import pandas as pd
    # from bubble_model.LPPLS_WYQ import LPPLS, data_in, mses1_compute, lppls_compute
    # from matplotlib import pyplot as plt
    #
    # # 数据
    # ydata, n, tdata = data_in(t1, t2, file_dir1)
    # result = pd.read_excel(file_dir2)
    # result.breakday_predict.plot()
    # result.breakday_predict.hist(bins=100)
    # coef = result.iloc[20, :]
    #
    # # 实际和预测值
    # x1 = np.linspace(1, int(coef[0]), int(coef[0]))
    # y1 = ydata[-int(coef[0]):]
    # x2 = np.linspace(1, n+200, n+200)
    # y2 = lppls_compute(x2, coef[3], coef[4], coef[5], coef[6], coef[7], coef[8], coef[9])
    #
    # # 作图展示
    # f, ax = plt.subplots(1, 1, figsize=(6, 3))
    # ax.plot(x1, y1, c='b', ls='-')
    # ax.plot(x2, y2, c='r', ls='--')
    # ax.vlines(x=coef[3], ymin=min(y1)*0.9, ymax=max(y2)*1.1, colors='#FFA500', linestyles='--')
    # plt.show()
