import pandas as pd
import numpy as np
import time


class FactorMom1d(object):

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
        self.all_data['momt'] = self.all_data['s_dq_pctchange']
        self.all_data['mom0'] = self.all_data['s_dq_open'] / self.all_data['s_dq_preclose'] - 1
        self.all_data['mom1_4'] = self.all_data['s_dq_pctchange'] - self.all_data['mom0']
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        item = ['trade_dt', 's_info_windcode', 'momt']
        self.all_data[item].to_pickle(self.save_indir + 'factor_price_momt.pkl')
        item = ['trade_dt', 's_info_windcode', 'mom0']
        self.all_data[item].to_pickle(self.save_indir + 'factor_price_mom0.pkl')
        item = ['trade_dt', 's_info_windcode', 'mom1_4']
        self.all_data[item].to_pickle(self.save_indir + 'factor_price_mom1_4.pkl')

        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_dayindex.pkl'

    mom1d = FactorMom1d(file_indir, save_indir, file_name)
    mom1d.runflow()
