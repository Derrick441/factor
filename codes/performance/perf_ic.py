import pandas as pd
import time
import os


# 计算因子值与收益值之间相关系数：IC
class PerfIc(object):

    def __init__(self, indir_ret, indir_fac, name_fac, indir_perf, m):
        self.indir_ret = indir_ret
        self.indir_fac = indir_fac
        self.name_fac = name_fac
        self.indir_perf = indir_perf
        self.method = m
        print(self.name_fac)

    def filein(self):
        t = time.time()
        # 从dataflow中读取return数据
        self.ret = pd.read_pickle(self.indir_ret)
        # 从factor中取因子数据
        self.factor = pd.read_pickle(self.indir_fac + self.name_fac)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        # 数据合并
        self.data = pd.merge(self.factor, self.ret, how='left')
        # 设index
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        # 去除nan
        self.data.dropna(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def perf_single_ic(self, data, method):
        # IC
        if method == 'IC':
            ic = data.corr(method='pearson')
        # rank IC
        else:
            ic = data.corr(method='spearman')
        # 返回未来收益与因子的相关系数
        return ic.iloc[0, 1:]

    def compute_ic(self):
        t = time.time()
        # 计算每期因子ic或rank ic值
        self.result = self.data.groupby(level=0).apply(self.perf_single_ic, method=self.method)
        print('compute_ic running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 将因子ic结果存储在performance文件夹中
        self.result.to_pickle(self.indir_perf + self.method + '_' + self.name_fac)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        self.filein()
        self.datamanage()
        self.compute_ic()
        self.fileout()


if __name__ == '__main__':
    # dataflow中return数据地址
    indir_dataflow_ret = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\all_ret_sum.pkl'
    # factor地址和factor文件名
    indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    name_factor = 'factor_price_bi.pkl'
    # performance中ic地址
    indir_perf_ic = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
    # ic 或者 rank ic
    method = 'IC'
    # ic = PerfIc(indir_dataflow_ret, indir_factor, name_factor, indir_perf_ic, method)
    # ic.runflow()

    # 计算全部因子ic
    name_factors = os.listdir(indir_factor)
    for i in name_factors:
        ic = PerfIc(indir_dataflow_ret, indir_factor, i, indir_perf_ic, method)
        ic.runflow()

# 中性化ic--------------------------------------------------------------------------------------------------------------
    indir_dataflow_ret = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\all_ret_sum_neutral.pkl'
    indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'
    name_factor = 'neutral_factor_price_bi.pkl'
    indir_perf_ic = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
    method = 'IC'
    # ic = PerfIc(indir_dataflow_ret, indir_factor, name_factor, indir_perf_ic, method)
    # ic.runflow()

    # 计算全部因子ic
    name_factors = os.listdir(indir_factor)
    for i in name_factors:
        ic = PerfIc(indir_dataflow_ret, indir_factor, i, indir_perf_ic, method)
        ic.runflow()
