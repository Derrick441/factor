import pandas as pd
import numpy as np
import time


# 日均价偏差apb: 日的5分钟成交量加权平均价的平均价/日成交量加权的5分钟成交量加权平均价的平均价
class Apb(object):

    def __init__(self, file_indirs, file_names):
        self.file_indirs = file_indirs
        self.file_names = file_names

    def filein(self, indir, name):
        t = time.time()
        # 输入股价和成交量数据
        self.data = pd.read_pickle(indir + name)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 将0值替换为nan
        self.data = self.data.replace(0, np.nan)
        # 去除没有成交量的数据
        self.data_drop = self.data[self.data['volume'] != np.nan]
        # 5分钟成交量加权平均价
        self.data_drop['vwap'] = self.data_drop['amount'] / self.data_drop['volume']*10
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def dayin_apb(self, data):
        temp = data.copy()
        # 日的5分钟成交量加权平均价的平均价
        mean = temp['vwap'].mean()
        # 日的成交量加权的5分钟成交量加权平均价的平均价
        vol_sum = temp['volume'].sum()
        mean_vol_weight = (temp['volume'] / vol_sum * temp['vwap']).sum()
        return np.log(mean/mean_vol_weight)

    def factor_compute(self):
        t = time.time()
        # 计算apb
        self.result_part = self.data_drop.groupby(['s_info_windcode', 'trade_dt'])\
                                         .apply(self.dayin_apb)\
                                         .reset_index()\
                                         .rename(columns={0: 'apb'})
        print('factor_compute using time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.result_sum = []
        for i in self.file_names:
            print(i)
            self.filein(self.file_indirs[0], i)
            self.datamanage()
            self.factor_compute()
            self.result_sum.append(self.result_part)
        self.temp_result = pd.concat(self.result_sum)
        # 数据对齐
        self.all_data = pd.read_pickle(self.file_indirs[0] + 'all_dayindex.pkl')
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', 'apb']
        self.result[item].to_pickle(self.file_indirs[1] + 'factor_hq_apb.pkl')
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = ['D:\\wuyq02\\develop\\python\\data\\developflow\\all\\',
                  'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\']
    file_name = ['all_store_hqdata_2012_5_derive.pkl', 'all_store_hqdata_2013_5_derive.pkl',
                 'all_store_hqdata_2014_5_derive.pkl', 'all_store_hqdata_2015_5_derive.pkl',
                 'all_store_hqdata_2016_5_derive.pkl', 'all_store_hqdata_2017_5_derive.pkl',
                 'all_store_hqdata_2018_5_derive.pkl', 'all_store_hqdata_2019_5_derive.pkl']
    apb = Apb(file_indir, file_name)
    apb.runflow()

# apb.filein()
# apb.datamanage()
# apb.factor_compute()
