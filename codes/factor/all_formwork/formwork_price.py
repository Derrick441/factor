import pandas as pd
import numpy as np
import time


class FactorX(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def method(self, data, perid):
        temp = data.copy()
        result = np.nan
        return result

    def compute(self):
        t = time.time()
        # 因子计算
        self.factor_name = ''
        self.temp_result = self.data_dropna.groupby('s_info_windcode')\
                                           .apply(self.method, 20, self.factor_name)\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', self.factor_name]
        self.result[item].to_pickle(self.save_indir + 'factor_price_' + self.factor_name + '.pkl')
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
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_dayindex.pkl'

    fx = FactorX(file_indir, save_indir, file_name)
    fx.runflow()
