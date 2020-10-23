import pandas as pd
import numpy as np
import time
import os
from sklearn.preprocessing import StandardScaler


class Combine(object):

    def __init__(self, factor_indir, ic_indir, save_indir, factor_name, ic_names, save_name):
        self.factor_indir = factor_indir
        self.ic_indir = ic_indir
        self.save_indir = save_indir
        self.factor_name = factor_name
        self.ic_names = ic_names
        self.save_name = save_name

    def filein(self):
        t = time.time()
        # 因子
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        # ic
        self.ic1 = pd.read_csv(self.ic_indir + self.ic_names[0])
        self.ic5 = pd.read_csv(self.ic_indir + self.ic_names[1])
        self.ic10 = pd.read_csv(self.ic_indir + self.ic_names[2])
        self.ic20 = pd.read_csv(self.ic_indir + self.ic_names[3])
        self.ic60 = pd.read_csv(self.ic_indir + self.ic_names[4])
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # factor
        # self.factor = self.factor.dropna()
        self.factor.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.factor.replace([np.nan, np.inf, -np.inf], 0, inplace=True)
        # 因子标准化
        scaler = StandardScaler()
        temp = scaler.fit_transform(self.factor)
        # 转dataframe
        self.factor_std = pd.DataFrame(temp, index=self.factor.index, columns=self.factor.columns)
        self.factor_std.reset_index(inplace=True)

        # ic
        self.ic1['trade_dt'] = self.ic1['trade_dt'].apply(lambda x: str(x))
        self.ic1.set_index('trade_dt', inplace=True)
        self.ic5['trade_dt'] = self.ic5['trade_dt'].apply(lambda x: str(x))
        self.ic5.set_index('trade_dt', inplace=True)
        self.ic10['trade_dt'] = self.ic10['trade_dt'].apply(lambda x: str(x))
        self.ic10.set_index('trade_dt', inplace=True)
        self.ic20['trade_dt'] = self.ic20['trade_dt'].apply(lambda x: str(x))
        self.ic20.set_index('trade_dt', inplace=True)
        self.ic60['trade_dt'] = self.ic60['trade_dt'].apply(lambda x: str(x))
        self.ic60.set_index('trade_dt', inplace=True)
        # 移动平均
        self.ic1_roll = self.ic1.shift(1).rolling(20).mean()
        self.ic1_roll.reset_index(inplace=True)
        self.ic5_roll = self.ic5.shift(1).rolling(20).mean()
        self.ic5_roll.reset_index(inplace=True)
        self.ic10_roll = self.ic10.shift(1).rolling(20).mean()
        self.ic10_roll.reset_index(inplace=True)
        self.ic20_roll = self.ic20.shift(1).rolling(20).mean()
        self.ic20_roll.reset_index(inplace=True)
        self.ic60_roll = self.ic60.shift(1).rolling(20).mean()
        self.ic60_roll.reset_index(inplace=True)

        # 数据对齐
        self.factor1 = pd.merge(self.ic1_roll['trade_dt'], self.factor_std, how='left').replace(np.nan, 0)
        self.factor5 = pd.merge(self.ic5_roll['trade_dt'], self.factor_std, how='left').replace(np.nan, 0)
        self.factor10 = pd.merge(self.ic10_roll['trade_dt'], self.factor_std, how='left').replace(np.nan, 0)
        self.factor20 = pd.merge(self.ic20_roll['trade_dt'], self.factor_std, how='left').replace(np.nan, 0)
        self.factor60 = pd.merge(self.ic60_roll['trade_dt'], self.factor_std, how='left').replace(np.nan, 0)

        item = ['trade_dt', 's_info_windcode']
        self.ic1_merge = pd.merge(self.factor1[item], self.ic1_roll, how='left').replace(np.nan, 0)
        self.ic5_merge = pd.merge(self.factor1[item], self.ic5_roll, how='left').replace(np.nan, 0)
        self.ic10_merge = pd.merge(self.factor1[item], self.ic5_roll, how='left').replace(np.nan, 0)
        self.ic20_merge = pd.merge(self.factor1[item], self.ic5_roll, how='left').replace(np.nan, 0)
        self.ic60_merge = pd.merge(self.factor1[item], self.ic5_roll, how='left').replace(np.nan, 0)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def compute(self):
        t = time.time()
        self.factor1['combine'] = np.sum(self.factor1 * self.ic1_merge.iloc[:, 3:], axis=1)
        self.factor5['combine'] = np.sum(self.factor5 * self.ic5_merge.iloc[:, 3:], axis=1)
        self.factor10['combine'] = np.sum(self.factor10 * self.ic10_merge.iloc[:, 3:], axis=1)
        self.factor20['combine'] = np.sum(self.factor20 * self.ic20_merge.iloc[:, 3:], axis=1)
        self.factor60['combine'] = np.sum(self.factor60 * self.ic60_merge.iloc[:, 3:], axis=1)
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        item = ['trade_dt', 's_info_windcode', 'combine']
        self.factor1[item].to_pickle(self.save_indir + self.save_name + '1.pkl')
        self.factor5[item].to_pickle(self.save_indir + self.save_name + '5.pkl')
        self.factor10[item].to_pickle(self.save_indir + self.save_name + '10.pkl')
        self.factor20[item].to_pickle(self.save_indir + self.save_name + '20.pkl')
        self.factor60[item].to_pickle(self.save_indir + self.save_name + '60.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\'
    ic_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_combine\\'
    factor_name = 'factor_all.pkl'
    ic_names = ['ic_all_factors_ret1.csv',
                'ic_all_factors_ret5.csv',
                'ic_all_factors_ret10.csv',
                'ic_all_factors_ret20.csv',
                'ic_all_factors_ret60.csv']
    save_name = 'combine_facor_1219_ic'
    co = Combine(factor_indir, ic_indir, save_indir, factor_name, ic_names, save_name)
    co.runflow()
