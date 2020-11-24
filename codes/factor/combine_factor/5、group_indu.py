import pandas as pd
import numpy as np
import time
import os


class GroupTen(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_names, factor_name, perid):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_names = file_names
        self.factor_name = factor_name
        self.perid = perid

    def filein(self):
        t = time.time()
        # 股票收益数据
        self.all_ret = pd.read_pickle(self.file_indir + self.file_names[0])
        # 行业数据
        self.all_indu = pd.read_pickle(self.file_indir + self.file_names[1])
        # 因子
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 因子名
        self.fac_name = self.factor_name[:-4]
        self.factor.rename(columns={self.factor.columns[-1]: self.fac_name}, inplace=True)

        # 数据合并
        temp = self.all_ret.reset_index().rename(columns={0: 'ret'})
        self.data = pd.merge(temp, self.all_indu, how='left')
        self.data = pd.merge(self.data, self.factor, how='left')
        # 去除空值
        item = ['trade_dt', 's_info_windcode', 'ret', 'induname1', self.fac_name]
        self.data_dropna = self.data[item].dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def groupten(self, data, index):
        temp = data.copy()
        temp['group'] = pd.cut(temp[index].rank(), 10, labels=range(10))
        result = temp.groupby('group')['ret'].mean()
        return result.values

    def compute(self):
        t = time.time()
        self.result = self.data_dropna.groupby(['trade_dt', 'induname1'])\
                                      .apply(self.groupten, self.fac_name)\
                                      .apply(pd.Series)\
                                      .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        dayret = self.result.groupby('trade_dt').mean() / self.perid
        save_name = self.fac_name + '_group_indu_ret' + str(self.perid) + '.csv'
        dayret.to_csv(self.save_indir + 'dayret\\' + save_name)
        print(self.result.mean() * 240 / 1)

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
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_combine\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\group_ret\\'

    file_names = ['all_band_adjvwap_hh_price_label1.pkl', 'all_band_indu.pkl']
    # factor_names = ['combine_factor_ic1.pkl']
    factor_names = ['combine_factor_ic1.pkl', 'combine_factor_ic5.pkl', 'combine_factor_ic10.pkl',
                    'combine_factor_ic20.pkl', 'combine_factor_ic60.pkl']

    # 1日收益
    factor = []
    result = []
    result_1 = []
    result_3 = []
    result_5 = []
    for factor_name in factor_names:
        print(factor_name)
        g10 = GroupTen(file_indir, factor_indir, save_indir, file_names, factor_name, 1)
        g10.runflow()
        factor.append(g10.fac_name)
        result.append(g10.result.mean() * 240 / 1)
        result_1.append(g10.result[g10.result.trade_dt >= '20150101'].mean() * 240 / 1)
        result_3.append(g10.result[g10.result.trade_dt >= '20170101'].mean() * 240 / 1)
        result_5.append(g10.result[g10.result.trade_dt >= '20190101'].mean() * 240 / 1)
    # 格式整理
    out = pd.concat(result, axis=1).T
    out.rename(columns={'trade_dt': 'factor'}, inplace=True)
    out['factor'] = factor
    out.to_csv(save_indir + 'change_group_mean1_combine_indu.csv')

    # 格式整理
    out_1 = pd.concat(result_1, axis=1).T
    out_1.rename(columns={'trade_dt': 'factor'}, inplace=True)
    out_1['factor'] = factor
    out_1.to_csv(save_indir + 'change_group_mean1_combine_1_indu.csv')

    # 格式整理
    out_3 = pd.concat(result_3, axis=1).T
    out_3.rename(columns={'trade_dt': 'factor'}, inplace=True)
    out_3['factor'] = factor
    out_3.to_csv(save_indir + 'change_group_mean1_combine_3_indu.csv')

    # 格式整理
    out_5 = pd.concat(result_5, axis=1).T
    out_5.rename(columns={'trade_dt': 'factor'}, inplace=True)
    out_5['factor'] = factor
    out_5.to_csv(save_indir + 'change_group_mean1_combine_5_indu.csv')
