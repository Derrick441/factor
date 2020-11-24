import pandas as pd
import numpy as np
import time
from sklearn.preprocessing import StandardScaler


class Combine(object):

    def __init__(self, factor_indir, ic_indir, save_indir, factor_name, ic_name, save_name, num):
        self.factor_indir = factor_indir
        self.ic_indir = ic_indir
        self.save_indir = save_indir

        self.factor_name = factor_name
        self.ic_name = ic_name
        self.save_name = save_name
        self.num = num
        print(self.ic_name)

    def filein(self):
        t = time.time()
        # 因子
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        # ic
        self.ic = pd.read_csv(self.ic_indir + self.ic_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # factor
        self.factor.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.factor.replace([np.nan, np.inf, -np.inf], 0, inplace=True)
        # 因子标准化
        scaler = StandardScaler()
        temp = scaler.fit_transform(self.factor)
        # 转dataframe
        self.factor_std = pd.DataFrame(temp, index=self.factor.index, columns=self.factor.columns)
        self.factor_std.reset_index(inplace=True)

        # ic
        self.ic.drop('Unnamed: 0', axis=1, inplace=True)
        self.ic.replace([np.nan, np.inf, -np.inf], 0, inplace=True)
        self.ic['trade_dt'] = self.ic['trade_dt'].apply(lambda x: str(x))
        self.ic.set_index('trade_dt', inplace=True)
        # 移动平均
        self.ic_roll = self.ic.shift(self.num).rolling(20).mean()
        self.ic_roll.reset_index(inplace=True)

        # 数据对齐
        self.factor_merge = pd.merge(self.ic_roll['trade_dt'], self.factor_std, how='left').replace(np.nan, 0)
        item = ['trade_dt', 's_info_windcode']
        self.ic_merge = pd.merge(self.factor_merge[item], self.ic_roll, how='left').replace(np.nan, 0)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def compute(self):
        t = time.time()
        self.factor_merge['combine'] = np.sum(self.factor_merge.iloc[:, 2:].values * self.ic_merge.iloc[:, 2:].values, axis=1)
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        item = ['trade_dt', 's_info_windcode', 'combine']
        self.factor_merge[item].to_pickle(self.save_indir + self.save_name)
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
    ic_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\all_ic\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_combine\\'
    factor_name = 'factor_all.pkl'

    ic_name = 'ic1_all.csv'
    save_name = 'combine_factor_ic1.pkl'
    num = 1
    co = Combine(factor_indir, ic_indir, save_indir, factor_name, ic_name, save_name, num)
    co.runflow()

    ic_name = 'ic5_all.csv'
    save_name = 'combine_factor_ic5.pkl'
    num = 5
    co = Combine(factor_indir, ic_indir, save_indir, factor_name, ic_name, save_name, num)
    co.runflow()

    ic_name = 'ic10_all.csv'
    save_name = 'combine_factor_ic10.pkl'
    num = 10
    co = Combine(factor_indir, ic_indir, save_indir, factor_name, ic_name, save_name, num)
    co.runflow()

    ic_name = 'ic20_all.csv'
    save_name = 'combine_factor_ic20.pkl'
    num = 20
    co = Combine(factor_indir, ic_indir, save_indir, factor_name, ic_name, save_name, num)
    co.runflow()

    ic_name = 'ic60_all.csv'
    save_name = 'combine_factor_ic60.pkl'
    num = 60
    co = Combine(factor_indir, ic_indir, save_indir, factor_name, ic_name, save_name, num)
    co.runflow()
