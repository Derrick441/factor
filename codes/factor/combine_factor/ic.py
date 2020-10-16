import pandas as pd
import numpy as np
import time
import os


# 计算因子值与收益值之间相关系数：IC
class PerfIc(object):

    def __init__(self, file_indir1, file_name1, file_indir2, file_name2, save_indir, method, neutral):
        self.file_indir1 = file_indir1
        self.file_name1 = file_name1
        self.file_indir2 = file_indir2
        self.file_name2 = file_name2
        self.save_indir = save_indir
        self.method = method
        self.neutral = neutral
        print(self.file_name1)
        print(self.file_name2)

    def filein(self):
        t = time.time()
        # 从factor中取因子数据
        self.fac = pd.read_pickle(self.file_indir1 + self.file_name1)
        # 从dataflow中读取return数据
        self.ret = pd.read_pickle(self.file_indir2 + self.file_name2)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        # 取因子名
        self.fac_name = self.fac.columns[-1]
        # 收益率reset_index()
        self.ret_reset = self.ret.reset_index().rename(columns={0: 'ret'})
        # 数据合并
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
        print('compute_ic running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 识别因子是否中性化
        if self.neutral == 0:
            self.save = self.method + '_' + self.file_name1[:-4] + '_' + self.file_name2[31:]
        else:
            self.save = self.method + '_' + self.file_name1[8:-4] + '_' + self.file_name2[4:]
        # 数据输出
        self.result.to_pickle(self.save_indir + self.save)
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
    file_names1 = os.listdir(file_indir1)
    file_indir2 = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_names2 = ['ret_1_neutral.pkl',
                   'ret_5_neutral.pkl',
                   'ret_10_neutral.pkl',
                   'ret_20_neutral.pkl',
                   'ret_60_neutral.pkl']
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
    method = 'IC'  # 计算IC或者rank IC
    neutral = 1    # 帮助文件命名

    # 计算全部因子ic
    for i in file_names1:
        for j in file_names2:
            ic = PerfIc(file_indir1, i, file_indir2, j, save_indir, method, neutral)
            ic.runflow()

    # file_names1 = ['combine_factor_ic_1.pkl', 'combine_factor_ic_5.pkl']
    # for i in file_names1:
    #     for j in file_names2:
    #         ic = PerfIc(file_indir1, i, file_indir2, j, save_indir, method, neutral)
    #         ic.runflow()
