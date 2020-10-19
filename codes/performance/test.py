import pandas as pd
import numpy as np
import time
import os


# 计算因子值与收益值之间相关系数：IC
class PerfIc(object):

    def __init__(self, file_indir1, file_indir2, save_indir, factor_name, ret_name, method, neutral):
        self.file_indir1 = file_indir1
        self.file_indir2 = file_indir2
        self.save_indir = save_indir
        self.factor_name = factor_name
        self.ret_name = ret_name
        self.method = method
        self.neutral = neutral
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
        # 收益率reset_index()
        self.ret_reset = self.ret.reset_index().rename(columns={0: 'ret1'})
        # 数据合并
        self.data = pd.merge(self.fac, self.ret_reset, how='left')
        self.data.sort_values(['s_info_windcode', 'trade_dt'], inplace=True)
        self.data['ret2'] = self.data.groupby('s_info_windcode')['ret1'].shift(-1)
        self.data['ret3'] = self.data.groupby('s_info_windcode')['ret1'].shift(-2)
        self.data['ret4'] = self.data.groupby('s_info_windcode')['ret1'].shift(-3)
        self.data['ret5'] = self.data.groupby('s_info_windcode')['ret1'].shift(-4)
        self.data['ret6'] = self.data.groupby('s_info_windcode')['ret1'].shift(-5)
        self.data['ret7'] = self.data.groupby('s_info_windcode')['ret1'].shift(-6)
        self.data['ret8'] = self.data.groupby('s_info_windcode')['ret1'].shift(-7)
        self.data['ret9'] = self.data.groupby('s_info_windcode')['ret1'].shift(-8)
        self.data['ret10'] = self.data.groupby('s_info_windcode')['ret1'].shift(-9)
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
        return ic.values[0, :]

    def compute_ic(self):
        t = time.time()
        # ic名
        self.ic_name = self.fac_name + '_' + self.method
        # 计算每期因子的ic或rank ic值
        self.result = self.data.groupby(level=0)\
                               .apply(self.perf_single_ic, method=self.method)\
                               .apply(pd.Series)\
                               .reset_index()
        self.result.sort_values('trade_dt', inplace=True)
        print('compute_ic running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 识别是否中性化
        if self.neutral == 0:
            self.save = self.factor_name[:-4] + '_rankic' + self.ret_name[31:]
        else:
            self.save = self.factor_name[:-4] + '_rankic' + self.ret_name[4:-12] + '.pkl'
        # 数据输出
        self.result.to_pickle(self.save_indir + self.save)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute_ic()
        # self.fileout()
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir1 = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_indir2 = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\rankic\\'

    file_names1 = ['factor_hq_apb1d.pkl']
    file_names2 = ['all_band_adjvwap_hh_price_label1.pkl']

    method = 'rankIC'
    neutral = 0

    # 计算全部因子ic
    for factor_name in file_names1:
        for ret_name in file_names2:
            ic = PerfIc(file_indir1, file_indir2, save_indir, factor_name, ret_name, method, neutral)
            ic.runflow()

    x = pd.DataFrame(ic.result.mean()).T
    x.to_csv('D:\\wuyq02\\develop\\python\\data\\performance\\rankic\\' + 'apb.csv')
