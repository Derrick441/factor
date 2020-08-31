import pandas as pd
import time


# 日指标衍生出5分钟指标
class DaytoFivemin(object):

    def __init__(self, file_indir, file_d, file_f):
        self.file_indir = file_indir
        self.file_d = file_d
        self.file_f = file_f

    def filein(self):
        t = time.time()
        self.data_day = pd.read_pickle(self.file_indir + self.file_d)
        self.data_five = pd.read_pickle(self.file_indir + self.file_f)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamange(self):
        t = time.time()
        self.data_day['closeprice_before'] = self.data_day.groupby('s_info_windcode').s_dq_close_today.shift(1)
        temp_data = self.data_day[['s_info_windcode', 'trade_dt', 's_dq_mv', 's_val_pe_ttm', 'closeprice_before']]
        self.data_all = pd.merge(self.data_five, temp_data, how='left')
        self.data_all['mv'] = self.data_all.s_dq_mv/self.data_all.closeprice_before*self.data_all.closeprice
        self.data_all['pe'] = self.data_all.s_val_pe_ttm/self.data_all.closeprice_before*self.data_all.closeprice
        self.data_all['change'] = (self.data_all.closeprice - self.data_all.openprice) / self.data_all.openprice * 100
        self.result = self.data_all.drop(['s_dq_mv', 's_val_pe_ttm', 'closeprice_before'], axis=1)
        print('datamanage running time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        self.result.to_pickle(indir + self.file_f[0:23] + '_derive.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamange()
        self.fileout()
        print('end\nusing time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_day = 'all_dayindex.pkl'
    # file_five = ['all_store_hqdata_2012_5.pkl']
    file_five = ['all_store_hqdata_2012_5.pkl', 'all_store_hqdata_2013_5.pkl', 'all_store_hqdata_2014_5.pkl',
                 'all_store_hqdata_2015_5.pkl', 'all_store_hqdata_2016_5.pkl', 'all_store_hqdata_2017_5.pkl',
                 'all_store_hqdata_2018_5.pkl', 'all_store_hqdata_2019_5.pkl', 'all_store_hqdata_2020_5.pkl']
    for i in file_five:
        print(i[-10:-6])
        df = DaytoFivemin(indir, file_day, i)
        df.runflow()
