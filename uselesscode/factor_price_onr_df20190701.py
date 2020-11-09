import pandas as pd
import numpy as np
import time


class ReturnTwoIndex1(object):

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
        self.data_dropna = self.all_data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def compute(self):
        t = time.time()
        self.all_data['onr'] = self.all_data['s_dq_open'] / self.all_data['s_dq_preclose'] - 1
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.all_data, how='left')
        item = ['trade_dt', 's_info_windcode', 'onr']
        self.result[item].to_pickle(self.save_indir + 'factor_price_onr.pkl')
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

    rti1 = ReturnTwoIndex1(file_indir, save_indir, file_name)
    rti1.runflow()
