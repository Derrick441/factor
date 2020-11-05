import pandas as pd
import numpy as np
import time


class FactorNmom(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        self.all_intarday = pd.read_pickle(self.file_indir + self.file_name[0])
        self.all_overnight = pd.read_pickle(self.file_indir + self.file_name[1])
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.all_data = pd.merge(self.all_intarday, self.all_overnight, how='left')
        self.data_dropna = self.all_data.dropna().copy()
        # self.data_dropna = self.data_dropna[self.data_dropna.s_info_windcode < '000010.SZ']
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def mom_meanstd(self, data, index1, index2):
        temp = data.copy()
        return np.mean(temp[index1]), np.std(temp[index1]), np.mean(temp[index2]), np.std(temp[index2])

    def compute(self):
        t = time.time()
        item = ['momi', 'momo']
        self.temp_meanstd = self.all_data.groupby('trade_dt')\
                                         .apply(self.mom_meanstd, item[0], item[1])\
                                         .apply(pd.Series)\
                                         .reset_index()\
                                         .rename(columns={0: 'momi_mean', 1: 'momi_std',
                                                          2: 'momo_mean', 3: 'momo_std'})
        self.temp_result = pd.merge(self.all_data, self.temp_meanstd, how='left')
        self.temp_result['nmomi'] = ((self.temp_result.momi-self.temp_result.momi_mean)/self.temp_result.momi_std)
        self.temp_result['nmomo'] = ((self.temp_result.momo-self.temp_result.momo_mean)/self.temp_result.momo_std)
        self.temp_result['nmom'] = self.temp_result['nmomi'] + self.temp_result['nmomo']
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', 'nmom']
        self.result[item].to_pickle(self.save_indir + 'factor_price_nmom.pkl')
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
    file_name = ['factor_price_momi.pkl', 'factor_price_momo.pkl']

    nmom = FactorNmom(file_indir, save_indir, file_name)
    nmom.runflow()
