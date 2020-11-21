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
        self.ret = pd.read_pickle(self.file_indir + self.file_name)
        self.fac = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.ret_reset = self.ret.reset_index().rename(columns={0: 'ret'})
        self.data = pd.merge(self.ret_reset, self.fac, how='left')
        self.data.sort_values(by=['trade_dt', 's_info_windcode'], inplace=True)
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data.dropna(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def perf_single_ic(self, data, m):
        temp = data.copy()
        # IC
        if m == 'IC':
            result = temp.corr(method='pearson')
        # rank IC
        else:
            result = temp.corr(method='spearman')
        # 返回未来收益与因子的相关系数
        return result.values[0, 1]

    def compute(self):
        t = time.time()
        self.ic_name = self.fac.columns[-1] + '_' + self.method
        self.result = self.data.groupby(level=0)\
                               .apply(self.perf_single_ic, self.method)\
                               .reset_index()\
                               .rename(columns={0: self.ic_name})
        self.result.sort_values('trade_dt', inplace=True)
        self.result['accu'] = self.result[self.ic_name].cumsum()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 识别
        if self.method == 'IC':
            if self.neutral == 0:
                self.save = self.factor_name[:-4] + '_ic' + self.file_name[31:-4] + '.csv'
            else:
                self.save = self.factor_name[:-4] + '_ic' + self.file_name[4:-12] + '.csv'
        else:
            if self.neutral == 0:
                self.save = self.factor_name[:-4] + '_rankic' + self.file_name[31:-4] + '.csv'
            else:
                self.save = self.factor_name[:-4] + '_rankic' + self.file_name[4:-12] + '.csv'
        # 数据输出
        self.result.to_csv(self.save_indir + self.save)
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
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_combine\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\combine\\'
    file_names = ['all_band_adjvwap_hh_price_label1.pkl',
                  'all_band_adjvwap_hh_price_label5.pkl',
                  'all_band_adjvwap_hh_price_label10.pkl',
                  'all_band_adjvwap_hh_price_label20.pkl',
                  'all_band_adjvwap_hh_price_label60.pkl']
    method = 'IC'
    neutral = 0

    factor_names = ['combine_factor_ic1.pkl', 'combine_factor_ic5.pkl',
                    'combine_factor_ic10.pkl', 'combine_factor_ic20.pkl', 'combine_factor_ic60.pkl']
    # factor_names = ['combine_select1_ic1.pkl', 'combine_select1_ic5.pkl',
    #                 'combine_select1_ic10.pkl', 'combine_select1_ic20.pkl', 'combine_select1_ic60.pkl']
    # factor_names = ['combine_select2_ic1.pkl', 'combine_select2_ic5.pkl',
    #                 'combine_select2_ic10.pkl', 'combine_select2_ic20.pkl', 'combine_select2_ic60.pkl']

    # 计算少数几个因子ic
    for factor_name in factor_names:
        for file_name in file_names:
            ic = PerfIc(file_indir, factor_indir, save_indir, file_name, factor_name, method, neutral)
            ic.runflow()
