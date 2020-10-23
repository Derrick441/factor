import pandas as pd
import numpy as np
import time
import os


# 计算因子值与收益值之间相关系数：IC
class PerfIc(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_name, factor_name, method, neutral):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_name = file_name
        self.factor_name = factor_name
        self.method = method
        self.neutral = neutral
        print(self.factor_name)
        print(self.file_name)

    def filein(self):
        t = time.time()
        # 从dataflow中取收益数据
        self.ret = pd.read_pickle(self.file_indir + self.file_name)
        # 从factor中取因子数据
        self.fac = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        # 收益率reset_index
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

    def compute(self):
        t = time.time()
        # ic名
        self.fac_name = self.fac.columns[-1]
        self.ic_name = self.fac_name + '_' + self.method
        # 计算每期因子的ic或rank ic值
        self.result = self.data.groupby(level=0)\
                               .apply(self.perf_single_ic, method=self.method)\
                               .reset_index()\
                               .rename(columns={0: self.ic_name})
        self.result.sort_values('trade_dt', inplace=True)
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 识别
        if self.method == 'IC':
            if self.neutral == 0:
                self.save = self.factor_name[:-4] + '_ic' + self.file_name[31:]
            else:
                self.save = self.factor_name[:-4] + '_ic' + self.file_name[4:-12] + '.pkl'
        else:
            if self.neutral == 0:
                self.save = self.factor_name[:-4] + '_rankic' + self.file_name[31:]
            else:
                self.save = self.factor_name[:-4] + '_rankic' + self.file_name[4:-12] + '.pkl'
        # 数据输出
        self.result.to_pickle(self.save_indir + self.save)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'

    file_names = ['all_band_adjvwap_hh_price_label1.pkl',
                  'all_band_adjvwap_hh_price_label5.pkl',
                  'all_band_adjvwap_hh_price_label10.pkl',
                  'all_band_adjvwap_hh_price_label20.pkl',
                  'all_band_adjvwap_hh_price_label60.pkl']
    # file_names = ['ret_1_neutral.pkl',
    #               'ret_5_neutral.pkl',
    #               'ret_10_neutral.pkl',
    #               'ret_20_neutral.pkl',
    #               'ret_60_neutral.pkl']
    factor_names = os.listdir(factor_indir)

    method = 'IC'
    neutral = 0

    # 计算全部因子ic
    for factor_name in factor_names:
        for file_name in file_names:
            ic = PerfIc(file_indir, factor_indir, save_indir, file_name, factor_name, method, neutral)
            ic.runflow()

    # # 计算未计算ic因子的ic
    # set1 = set(os.listdir('D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'))
    # temp_set = os.listdir('D:\\wuyq02\\develop\\python\\data\\performance\\ic\\')
    # set2 = set([ic_name.split('_ic')[0] + '.pkl' for ic_name in temp_set])
    # factor_names = set1 - set2
    #
    # for factor_name in factor_names:
    #     for file_name in file_names:
    #         ic = PerfIc(file_indir, factor_indir, save_indir, file_name, factor_name, method, neutral)
    #         ic.runflow()
