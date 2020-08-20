import pandas as pd
import time


# 计算因子值与收益值之间相关系数
class PerformanceIc(object):

    def __init__(self, indir_d, name_d, indir_f, name_f, indir_p, method):
        self.indir_d = indir_d
        self.name_d = name_d
        self.indir_f = indir_f
        self.name_f = name_f
        self.indir_p = indir_p
        self.method = method

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中读取return数据
        self.ret = pd.read_pickle(self.indir_d + self.name_d)
        # 从factor文件夹中取因子数据
        self.factor = pd.read_pickle(self.indir_f + self.name_f)
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
        self.result.to_pickle(self.indir_p + self.method + '_' + self.name_f)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        self.filein()
        self.datamanage()
        self.compute_ic()
        self.fileout()


# if __name__ == '__main__':
#     # dataflow数据地址：收益数据读取
#     indir_dataflow = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
#     name_dataflow = 'all_ret_sum.pkl'
#     # factor数据地址：因子数据读取
#     indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
#     name_factor = 'factor_price_ivff.pkl'
#     # performance数据地址：ic数据存放
#     indir_perf = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
#
#     performanceic = PerformanceIc(indir_dataflow, name_dataflow, indir_factor, name_factor, indir_perf, 'IC')
#     performanceic.runflow()
