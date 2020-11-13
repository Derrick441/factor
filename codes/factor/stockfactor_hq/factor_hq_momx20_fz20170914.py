import pandas as pd
import numpy as np
import time


# 因子：一日收益拆分5部分,20累加动量
class FactorMomx(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name
        print(self.file_name)

    def filein(self):
        t = time.time()
        self.data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.data.sort_values(by=['s_info_windcode', 'trade_dt', 'bargaintime'], inplace=True)
        self.data_dropna = self.data.dropna().copy()
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def compute(self):
        t = time.time()
        self.data_mom = self.data_dropna[0::4].copy()
        self.data_mom['mom1_temp'] = (self.data_mom.closeprice/self.data_mom.openprice - 1)*100
        self.data_mom['mom2_temp'] = (self.data_dropna[1::4].closeprice/self.data_dropna[1::4].openprice - 1).values*100
        self.data_mom['mom3_temp'] = (self.data_dropna[2::4].closeprice/self.data_dropna[2::4].openprice - 1).values*100
        self.data_mom['mom4_temp'] = (self.data_dropna[3::4].closeprice/self.data_dropna[3::4].openprice - 1).values*100
        # 滚动累加
        self.data_mom['mom1_20'] = self.data_mom.groupby('s_info_windcode')['mom1_temp'].rolling(20).sum().values
        self.data_mom['mom2_20'] = self.data_mom.groupby('s_info_windcode')['mom2_temp'].rolling(20).sum().values
        self.data_mom['mom3_20'] = self.data_mom.groupby('s_info_windcode')['mom3_temp'].rolling(20).sum().values
        self.data_mom['mom4_20'] = self.data_mom.groupby('s_info_windcode')['mom4_temp'].rolling(20).sum().values
        print('compute using time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        item = ['trade_dt', 's_info_windcode', 'mom1_20']
        self.data_mom[item].to_pickle(self.save_indir + 'factor_hq_mom1_20_' + self.file_name[17:21] + '.pkl')

        item = ['trade_dt', 's_info_windcode', 'mom2_20']
        self.data_mom[item].to_pickle(self.save_indir + 'factor_hq_mom2_20_' + self.file_name[17:21] + '.pkl')

        item = ['trade_dt', 's_info_windcode', 'mom3_20']
        self.data_mom[item].to_pickle(self.save_indir + 'factor_hq_mom3_20_' + self.file_name[17:21] + '.pkl')

        item = ['trade_dt', 's_info_windcode', 'mom4_20']
        self.data_mom[item].to_pickle(self.save_indir + 'factor_hq_mom4_20_' + self.file_name[17:21] + '.pkl')
        print('fileout using time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\annual_factor\\'
    file_names = ['all_store_hqdata_2012_5_1h.pkl', 'all_store_hqdata_2013_5_1h.pkl',
                  'all_store_hqdata_2014_5_1h.pkl', 'all_store_hqdata_2015_5_1h.pkl',
                  'all_store_hqdata_2016_5_1h.pkl', 'all_store_hqdata_2017_5_1h.pkl',
                  'all_store_hqdata_2018_5_1h.pkl', 'all_store_hqdata_2019_5_1h.pkl']

    for file_name in file_names:
        momx = FactorMomx(file_indir, save_indir, file_name)
        momx.runflow()

    def merge_data(factor_name, names):
        readin_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\annual_factor\\'
        saveout_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        # 数据读取合并
        data_sum = []
        for name in names:
            data_sum.append(pd.read_pickle(readin_indir + name))
        temp_result = pd.concat(data_sum)
        # 数据对齐
        all_data = pd.read_pickle(file_indir + 'all_dayindex.pkl')
        result = pd.merge(all_data[['trade_dt', 's_info_windcode']], temp_result, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', factor_name]
        result[item].to_pickle(saveout_indir + 'factor_hq_' + factor_name + '.pkl')

    factor_name = 'mom1_20'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'mom2_20'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'mom3_20'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'mom4_20'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)
