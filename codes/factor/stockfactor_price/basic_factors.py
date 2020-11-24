import pandas as pd
import numpy as np
import time


# 基础量价因子：当日自由流通市值(对数)、当日换手率(基于自由流通市值）、当日x日收益率动量、当日x日收益率波动率
class FactorBasic(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        item = ['s_info_windcode', 'trade_dt', 's_dq_freemv', 's_dq_freeturnover', 's_dq_pctchange']
        self.data = self.all_data[item].sort_values(['s_info_windcode', 'trade_dt'])
        self.data_dropna = self.data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def mom_of_n(self, data):
        temp = data.copy()
        temp_result = 1
        for i in temp:
            temp_result = temp_result * (1+i/100)
        return (temp_result-1)*100

    def compute(self):
        t = time.time()
        self.data_dropna['fmv'] = np.log(self.data_dropna['s_dq_freemv'])
        self.data_dropna.rename(columns={'s_dq_freeturnover': 'fto'}, inplace=True)
        self.data_dropna['mom5'] = self.data_dropna.groupby(['s_info_windcode'])['s_dq_pctchange']\
                                                   .rolling(5)\
                                                   .apply(self.mom_of_n)\
                                                   .values
        self.data_dropna['mom10'] = self.data_dropna.groupby(['s_info_windcode'])['s_dq_pctchange']\
                                                    .rolling(10)\
                                                    .apply(self.mom_of_n)\
                                                    .values
        self.data_dropna['mom20'] = self.data_dropna.groupby(['s_info_windcode'])['s_dq_pctchange']\
                                                    .rolling(20)\
                                                    .apply(self.mom_of_n)\
                                                    .values
        self.data_dropna['mom60'] = self.data_dropna.groupby(['s_info_windcode'])['s_dq_pctchange']\
                                                    .rolling(60)\
                                                    .apply(self.mom_of_n)\
                                                    .values
        self.data_dropna['mom120'] = self.data_dropna.groupby(['s_info_windcode'])['s_dq_pctchange']\
                                                     .rolling(120)\
                                                     .apply(self.mom_of_n)\
                                                     .values
        self.data_dropna['std10'] = self.data_dropna.groupby(['s_info_windcode'])['s_dq_pctchange']\
                                                    .rolling(10)\
                                                    .std()\
                                                    .values
        self.data_dropna['std20'] = self.data_dropna.groupby(['s_info_windcode'])['s_dq_pctchange']\
                                                    .rolling(20)\
                                                    .std()\
                                                    .values
        self.data_dropna['std60'] = self.data_dropna.groupby(['s_info_windcode'])['s_dq_pctchange']\
                                                    .rolling(60)\
                                                    .std()\
                                                    .values
        self.data_dropna['std120'] = self.data_dropna.groupby(['s_info_windcode'])['s_dq_pctchange']\
                                                     .rolling(120)\
                                                     .std()\
                                                     .values
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.data_dropna, how='left')
        
        item = ['trade_dt', 's_info_windcode', 'fmv']
        self.result[item].to_pickle(self.save_indir + 'factor_price_fmv.pkl')

        item = ['trade_dt', 's_info_windcode', 'fto']
        self.result[item].to_pickle(self.save_indir + 'factor_price_fto.pkl')

        item = ['trade_dt', 's_info_windcode', 'mom5']
        self.result[item].to_pickle(self.save_indir + 'factor_price_mom5.pkl')

        item = ['trade_dt', 's_info_windcode', 'mom10']
        self.result[item].to_pickle(self.save_indir + 'factor_price_mom10.pkl')

        item = ['trade_dt', 's_info_windcode', 'mom20']
        self.result[item].to_pickle(self.save_indir + 'factor_price_mom20.pkl')

        item = ['trade_dt', 's_info_windcode', 'mom60']
        self.result[item].to_pickle(self.save_indir + 'factor_price_mom60.pkl')

        item = ['trade_dt', 's_info_windcode', 'mom120']
        self.result[item].to_pickle(self.save_indir + 'factor_price_mom120.pkl')

        item = ['trade_dt', 's_info_windcode', 'std10']
        self.result[item].to_pickle(self.save_indir + 'factor_price_std10.pkl')

        item = ['trade_dt', 's_info_windcode', 'std20']
        self.result[item].to_pickle(self.save_indir + 'factor_price_std20.pkl')

        item = ['trade_dt', 's_info_windcode', 'std60']
        self.result[item].to_pickle(self.save_indir + 'factor_price_std60.pkl')

        item = ['trade_dt', 's_info_windcode', 'std120']
        self.result[item].to_pickle(self.save_indir + 'factor_price_std120.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

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

    fb = FactorBasic(file_indir, save_indir, file_name)
    fb.runflow()
