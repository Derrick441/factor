import pandas as pd
import numpy as np
import time
import os


# 计算因子值与收益值之间相关系数：IC
class PerfIc(object):

    def __init__(self, file_indir1, file_indir2, save_indir, factor_name, ret_name, method):
        self.file_indir1 = file_indir1
        self.file_indir2 = file_indir2
        self.save_indir = save_indir
        self.factor_name = factor_name
        self.ret_name = ret_name
        self.method = method
        print(self.factor_name)
        print(self.ret_name)

    def filein(self):
        t = time.time()
        # 从factor中取因子数据
        self.fac = pd.read_pickle(self.file_indir1 + self.factor_name)
        # 从dataflow中读取return数据
        self.ret = pd.read_pickle(self.file_indir2 + self.ret_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        # 取因子名
        self.fac_name = self.fac.columns[-1]
        # 数据合并
        self.ret_reset = self.ret.reset_index().rename(columns={0: 'ret'})
        self.data = pd.merge(self.fac, self.ret_reset, how='left')
        # 排序、设index
        self.data.sort_values(by=['trade_dt', 's_info_windcode'], inplace=True)
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
        return ic.values[0, 1]

    def compute_ic(self):
        t = time.time()
        # ic名称
        self.ic_name = self.fac_name + '_' + self.method
        # 计算每期因子的ic或rank ic值
        self.result = self.data.groupby(level=0)\
                               .apply(self.perf_single_ic, method=self.method)\
                               .reset_index()\
                               .rename(columns={0: self.ic_name})
        self.result.sort_values('trade_dt', inplace=True)
        print('compute_ic running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据输出
        self.save_name = self.method + '_' + self.factor_name[:-4] + '_' + self.ret_name[4:]
        self.result.to_pickle(self.save_indir + self.save_name)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute_ic()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir1 = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_combine\\'
    file_indir2 = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'

    file_names1 = ['factor_combine_ic1.pkl',
                   'factor_combine_ic5.pkl',
                   'factor_combine_ic10.pkl',
                   'factor_combine_ic20.pkl',
                   'factor_combine_ic60.pkl']
    # file_names1 = ['factor_combine_rankic1.pkl',
    #                'factor_combine_rankic5.pkl',
    #                'factor_combine_rankic10.pkl',
    #                'factor_combine_rankic20.pkl',
    #                'factor_combine_rankic60.pkl']
    file_names2 = ['ret_1_neutral.pkl',
                   'ret_5_neutral.pkl',
                   'ret_10_neutral.pkl',
                   'ret_20_neutral.pkl',
                   'ret_60_neutral.pkl']

    method = 'IC'  # 计算IC或者rank IC
    # method = 'RankIC'

    # 计算全部因子ic
    for factor_name in file_names1:
        for ret_name in file_names2:
            ic = PerfIc(file_indir1, file_indir2, save_indir, factor_name, ret_name, method)
            ic.runflow()
