from perf_ic import PerformanceIc
from perf_ic1 import PerformanceIc1
from perf_ic2 import PerformanceIc2
import pandas as pd
import os


# 全部因子计算ic--------------------------------------------------------------------------------------------------------
def ic_compute_save(flag, name_save, method):
    # dataflow数据地址
    indir_dataflow = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    # data数据的文件名
    name_dataflow_1 = 'all_ret_sum.pkl'
    name_dataflow_3 = ['all_ret_sum.pkl', 'all_dayindex.pkl', 'all_indu.pkl']
    # factor数据地址
    indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    # 所有因子的文件名
    factors = os.listdir(indir_factor)
    # performance数据存放地址
    indir_perf = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'

    mean = []
    coln = []
    for i in factors:
        if flag == 0:
            performance_ic = PerformanceIc(indir_dataflow, name_dataflow_1, indir_factor, i, indir_perf, method)
        elif flag == 1:
            performance_ic = PerformanceIc1(indir_dataflow, name_dataflow_3, indir_factor, i, indir_perf, method)
        else:
            performance_ic = PerformanceIc2(indir_dataflow, name_dataflow_3, indir_factor, i, indir_perf, method)
        # 计算每期ic值，并存储
        performance_ic.runflow()
        # 根据每期ic值，计算ic均值
        temp = performance_ic.result.mean()
        mean.append(temp)
        coln.append(temp.index.name)

    # 数据调整
    ic_all_factors = pd.concat(mean, axis=1)
    ic_all_factors.columns = coln
    ic_all_factors = ic_all_factors.T

    # ic均值汇总数据输出
    ic_all_factors.to_csv(indir_perf + name_save)

    return ic_all_factors


# ic
ic_mean = ic_compute_save(0, 'ic_mean_all_factors.csv', 'IC')
ic1_mean = ic_compute_save(1, 'ic1_mean_all_factors.csv', 'IC')
ic2_mean = ic_compute_save(2, 'ic2_mean_all_factors.csv', 'IC')

# # rank ic
# rank_ic_mean = ic_compute_save(0, 'ic_mean_all_factors.csv', 'rank IC')
# rank_ic1_mean = ic_compute_save(1, 'ic1_mean_all_factors.csv', 'rank IC')
# rank_ic2_mean = ic_compute_save(2, 'ic2_mean_all_factors.csv', 'rank IC')

# ---------------------------------------------------------------------------------------------------------------------#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# ---------------------------------------------------------------------------------------------------------------------#

# # 单个因子计算ic
#
# # 因子的文件名
# factor = 'factor_price_ivff.pkl'
# # IC or rank IC
# method = 'IC'
#
# # dataflow数据地址
# indir_dataflow = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
# # data数据的文件名
# name_dataflow_1 = 'all_ret_sum.pkl'
# name_dataflow_3 = ['all_ret_sum.pkl', 'all_dayindex.pkl', 'all_indu.pkl']
# # factor数据地址
# indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
# # performance数据存放地址
# indir_perf = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
#
# # performance_ic = PerformanceIc(indir_dataflow, name_dataflow_1, indir_factor, factor, indir_perf, method)
# performance_ic = PerformanceIc1(indir_dataflow, name_dataflow_3, indir_factor, factor, indir_perf, method)
# # performance_ic = PerformanceIc2(indir_dataflow, name_dataflow_3, indir_factor, factor, indir_perf, method)
# # 计算每期ic值，并存储
# performance_ic.runflow()
# # 根据每期ic值，计算ic均值
# temp = performance_ic.result.mean()
