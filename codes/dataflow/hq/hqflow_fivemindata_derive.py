import pandas as pd
import time


# 日指标衍生出5分钟指标
class DaytoFivemin(object):

    def __init__(self, file_indir, save_indir, file_name1, file_name2):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name1 = file_name1
        self.file_name2 = file_name2

    def filein(self):
        t = time.time()
        # 日数据
        self.data_all = pd.read_pickle(self.file_indir + self.file_name1)
        # 5分钟数据
        self.data_five = pd.read_pickle(self.file_indir + self.file_name2)
        self.data_five.sort_values(['s_info_windcode', 'trade_dt', 'bargaintime'], inplace=True)
        print('filein running time:%10.4fs' % (time.time()-t))

    def derive(self):
        t = time.time()
        temp_data = self.data_all[['s_info_windcode', 'trade_dt', 's_dq_freemv', 's_val_pb_new', 's_dq_close']].copy()
        # 合并
        self.data_sum = pd.merge(self.data_five, temp_data, how='left')
        self.data_sum['change'] = (self.data_sum.closeprice - self.data_sum.openprice) / self.data_sum.openprice * 100
        self.data_sum['mv'] = self.data_sum.s_dq_freemv/self.data_sum.s_dq_close*self.data_sum.closeprice
        self.data_sum['pb'] = self.data_sum.s_val_pb_new/self.data_sum.s_dq_close*self.data_sum.closeprice
        print('derive running time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        self.data_sum.to_pickle(self.save_indir + self.file_name2[:-4] + '_derive.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.derive()
        self.fileout()
        print('end running time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name1 = 'all_dayindex.pkl'
    file_name2s = ['all_store_hqdata_2012_5.pkl', 'all_store_hqdata_2013_5.pkl',
                   'all_store_hqdata_2014_5.pkl', 'all_store_hqdata_2015_5.pkl',
                   'all_store_hqdata_2016_5.pkl', 'all_store_hqdata_2017_5.pkl',
                   'all_store_hqdata_2018_5.pkl', 'all_store_hqdata_2019_5.pkl']
    for file_name2 in file_name2s:
        print(file_name2[-10:-6])
        df = DaytoFivemin(file_indir, save_indir, file_name1, file_name2)
        df.runflow()
