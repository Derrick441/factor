import pandas as pd
import numpy as np
import time
import statsmodels.api as sm


# 调整换手率：剔除换手率中市值影响
class AdjFreeTurnover(object):

    def __init__(self, file_indir, save_indir, file_names):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_names = file_names

    def filein(self):
        t = time.time()
        self.fto = pd.read_pickle(self.file_indir + self.file_names[0])
        self.fmv = pd.read_pickle(self.file_indir + self.file_names[1])
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.data = pd.merge(self.fmv, self.fto, how='left')
        self.data_dropna = self.data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def method(self, data, y_item, x_item, name):
        temp = data.copy()
        y = temp[y_item]
        x = temp[x_item]
        x['intercept'] = 1
        model = sm.OLS(y, x).fit()
        return pd.DataFrame({'s_info_windcode': temp.s_info_windcode.values, name: model.resid})

    def compute(self):
        t = time.time()
        self.temp_result = self.data_dropna.groupby('trade_dt')\
                                           .apply(self.method, ['fto'], ['fmv'], 'adjfto')\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.fto[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', 'adjfto']
        self.result[item].to_pickle(self.save_indir + 'factor_price_adjfto.pkl')
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
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_names = ['factor_price_fto.pkl', 'factor_price_fmv.pkl']

    adjfto = AdjFreeTurnover(file_indir, save_indir, file_names)
    adjfto.runflow()
