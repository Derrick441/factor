import pandas as pd
import numpy as np
import time


# 根据历史ic矩阵
class FactorMomf10(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_name, factor_names, m):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_name = file_name
        self.factor_names = factor_names
        self.m = m

    def filein(self):
        t = time.time()
        self.ret = pd.read_pickle(self.file_indir + self.file_name)
        self.data_mom0 = pd.read_pickle(self.factor_indir + self.factor_names[0])
        self.data_mom1 = pd.read_pickle(self.factor_indir + self.factor_names[1])
        self.data_mom2 = pd.read_pickle(self.factor_indir + self.factor_names[2])
        self.data_mom3 = pd.read_pickle(self.factor_indir + self.factor_names[3])
        self.data_mom4 = pd.read_pickle(self.factor_indir + self.factor_names[4])
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.ret = self.ret.reset_index().rename(columns={0: 'ret_10'})
        self.data_mom0.sort_values(by=['s_info_windcode', 'trade_dt'], inplace=True)
        self.data_mom0['mom0_10'] = self.data_mom0.groupby('s_info_windcode')['mom0'].rolling(10).sum().values
        self.data = pd.merge(self.ret, self.data_mom0, how='left')
        self.data = pd.merge(self.data, self.data_mom1, how='left')
        self.data = pd.merge(self.data, self.data_mom2, how='left')
        self.data = pd.merge(self.data, self.data_mom3, how='left')
        self.data = pd.merge(self.data, self.data_mom4, how='left')
        self.data_dropna = self.data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def method(self, data, item, m):
        temp = data[item].copy()
        # IC
        if m == 'IC':
            result = temp.corr(method='pearson')
        # rank IC
        else:
            result = temp.corr(method='spearman')
        # 返回未来收益与因子的相关系数
        return result.iloc[0, 1:]

    def compute(self):
        t = time.time()
        item = ['ret_10', 'mom0_10', 'mom1_10', 'mom2_10', 'mom3_10', 'mom4_10']
        self.IC = self.data_dropna.groupby('trade_dt')\
                                  .apply(self.method, item, self.m)\
                                  .apply(pd.Series)\
                                  .reset_index()
        item = ['mom0_10', 'mom1_10', 'mom2_10', 'mom3_10', 'mom4_10']
        self.IC_mean = self.IC[item].mean()
        self.IC_cov_inv = np.linalg.inv(np.cov(self.IC[item].dropna().T))
        self.weight = np.dot(self.IC_cov_inv, self.IC_mean)
        self.data_dropna['momf10'] = np.dot(self.data_dropna[item], self.weight)
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.data_mom0[['trade_dt', 's_info_windcode']], self.data_dropna, how='left')
        item = ['trade_dt', 's_info_windcode', 'momf10']
        self.result[item].to_pickle(self.save_indir + 'factor_hq_momf10.pkl')
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
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_band_adjvwap_hh_price_label10.pkl'
    factor_names = ['factor_price_mom0.pkl',
                    'factor_hq_mom1_10.pkl',
                    'factor_hq_mom2_10.pkl',
                    'factor_hq_mom3_10.pkl',
                    'factor_hq_mom4_10.pkl']
    method = 'IC'

    momf10 = FactorMomf10(file_indir, factor_indir, save_indir, file_name, factor_names, method)
    momf10.runflow()
