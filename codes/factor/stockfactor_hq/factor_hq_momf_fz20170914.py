import pandas as pd
import numpy as np
import time


class FactorMomf(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_name, factor_names, method):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_name = file_name
        self.factor_names = factor_names
        self.method = method

    def filein(self):
        t = time.time()
        # 股票日数据
        self.ret = pd.read_pickle(self.file_indir + self.file_name)
        self.data_mom0 = pd.read_pickle(self.factor_indir + self.factor_names[0])
        self.data_mom1 = pd.read_pickle(self.factor_indir + self.factor_names[1])
        self.data_mom2 = pd.read_pickle(self.factor_indir + self.factor_names[2])
        self.data_mom3 = pd.read_pickle(self.factor_indir + self.factor_names[3])
        self.data_mom4 = pd.read_pickle(self.factor_indir + self.factor_names[4])
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 调整
        self.ret = self.ret.reset_index().rename(columns={0: 'ret'})
        self.data_mom0.rename(columns={'onr': 'mom0'}, inplace=True)
        # 数据合并
        self.data = pd.merge(self.ret, self.data_mom0, how='left')
        self.data = pd.merge(self.data, self.data_mom1, how='left')
        self.data = pd.merge(self.data, self.data_mom2, how='left')
        self.data = pd.merge(self.data, self.data_mom3, how='left')
        self.data = pd.merge(self.data, self.data_mom4, how='left')
        # 去除空值
        self.data_dropna = self.data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def perf_ic(self, data, item, m):
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
        # 计算每期因子的ic或rank ic值
        item = ['ret', 'mom0', 'mom1', 'mom2', 'mom3', 'mom4']
        self.IC = self.data_dropna.groupby('trade_dt')\
                                  .apply(self.perf_ic, item, self.method)\
                                  .apply(pd.Series)\
                                  .reset_index()
        self.IC_mean = self.IC[['mom0', 'mom1', 'mom2', 'mom3', 'mom4']].mean()
        self.IC_cov_inv = np.linalg.inv(np.cov(self.IC[['mom0', 'mom1', 'mom2', 'mom3', 'mom4']].dropna().T))
        self.weight = np.dot(self.IC_cov_inv, self.IC_mean)
        self.data_dropna['momf'] = np.dot(self.data_dropna[['mom0', 'mom1', 'mom2', 'mom3', 'mom4']], self.weight)
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.data_mom0[['trade_dt', 's_info_windcode']], self.data_dropna, how='left')
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'momf']
        self.result[item].to_pickle(self.save_indir + 'factor_hq_momf.pkl')
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
    file_name = 'all_band_adjvwap_hh_price_label20.pkl'
    factor_names = ['factor_price_onr.pkl',
                    'factor_hq_mom1.pkl',
                    'factor_hq_mom2.pkl',
                    'factor_hq_mom3.pkl',
                    'factor_hq_mom4.pkl']
    method = 'IC'

    momf = FactorMomf(file_indir, factor_indir, save_indir, file_name, factor_names, method)
    momf.runflow()
