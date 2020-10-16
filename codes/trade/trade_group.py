import pandas as pd
import numpy as np
import time
import os


class GroupTen(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_names, factor_name):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_names = file_names
        self.factor_name = factor_name

    def filein(self):
        t = time.time()
        # 股票日数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_names[0])
        # 行业数据
        self.all_indu = pd.read_pickle(self.file_indir + self.file_names[1])
        # 因子
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 因子名
        self.factor_name = self.factor.columns[-1]
        # 数据合并
        temp = self.all_data.reset_index().rename(columns={0: 's_dq_pctchange'})
        self.data_sum = pd.merge(temp, self.all_indu, how='left')
        self.data_sum = pd.merge(self.data_sum, self.factor, how='left')
        # 去除空值
        item = ['trade_dt', 's_info_windcode', 's_dq_pctchange', 'induname1', self.factor_name]
        self.data_dropna = self.data_sum[item].dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def groupten(self, data, index):
        temp = data.copy()
        temp['group'] = pd.cut(temp[index].rank(), 10, labels=range(10))
        result = temp.groupby('group')['s_dq_pctchange'].mean()
        return result.values

    def compute(self):
        t = time.time()
        self.result = self.data_dropna.groupby(['trade_dt', 'induname1'])\
                                      .apply(self.groupten, self.factor_name)\
                                      .apply(pd.Series)\
                                      .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        print(self.result.mean() * 243)

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\test\\'
    file_names = ['all_band_adjvwap_hh_price_label1.pkl', 'all_band_indu.pkl']
    factor_names = os.listdir(factor_indir)

    # 1日收益
    result = []
    for factor_name in factor_names:
        print(factor_name)
        g10 = GroupTen(file_indir, factor_indir, save_indir, file_names, factor_name)
        g10.runflow()
        result.append(g10.result.mean() * 243)
    out = pd.concat(result)
    out.to_csv(save_indir + 'change_group_mean1.csv')

    # 5日收益
    file_names = ['all_band_adjvwap_hh_price_label5.pkl', 'all_band_indu.pkl']
    result = []
    for factor_name in factor_names:
        print(factor_name)
        g10 = GroupTen(file_indir, factor_indir, save_indir, file_names, factor_name)
        g10.runflow()
        result.append(g10.result.mean() * 243)
    out = pd.concat(result)
    out.to_csv(save_indir + 'change_group_mean5.csv')

    # 10日收益
    file_names = ['all_band_adjvwap_hh_price_label10.pkl', 'all_band_indu.pkl']
    result = []
    for factor_name in factor_names:
        print(factor_name)
        g10 = GroupTen(file_indir, factor_indir, save_indir, file_names, factor_name)
        g10.runflow()
        result.append(g10.result.mean() * 243)
    out = pd.concat(result)
    out.to_csv(save_indir + 'change_group_mean10.csv')

    # 20日收益
    file_names = ['all_band_adjvwap_hh_price_label20.pkl', 'all_band_indu.pkl']
    result = []
    for factor_name in factor_names:
        print(factor_name)
        g10 = GroupTen(file_indir, factor_indir, save_indir, file_names, factor_name)
        g10.runflow()
        result.append(g10.result.mean() * 243)
    out = pd.concat(result)
    out.to_csv(save_indir + 'change_group_mean20.csv')

    # 60日收益
    file_names = ['all_band_adjvwap_hh_price_label60.pkl', 'all_band_indu.pkl']
    result = []
    for factor_name in factor_names:
        print(factor_name)
        g10 = GroupTen(file_indir, factor_indir, save_indir, file_names, factor_name)
        g10.runflow()
        result.append(g10.result.mean() * 243)
    out = pd.concat(result)
    out.to_csv(save_indir + 'change_group_mean60.csv')
