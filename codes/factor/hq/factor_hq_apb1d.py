import pandas as pd
import numpy as np
import time


# 日均价偏差apb: 日的5分钟成交量加权平均价的平均价/日成交量加权的5分钟成交量加权平均价的平均价
class Apb(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 输入数据
        self.data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 将0值替换为nan
        self.data = self.data.replace(0, np.nan)
        # 去除成交量为nan的数据
        self.data_dropna = self.data[self.data['volume'] != np.nan].copy()
        # 5分钟成交量加权平均价
        self.data_dropna['vwap'] = self.data_dropna['amount'] / self.data_dropna['volume']*10
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def oneday_apb(self, data):
        temp = data.copy()
        # 平均价
        mean = temp['vwap'].mean()
        # 成交量加权平均价
        vol_sum = temp['volume'].sum()
        mean_weight = (temp['volume'] / vol_sum * temp['vwap']).sum()
        # 0值处理
        if (mean == 0) | (mean_weight == 0):
            result = np.nan
        else:
            result = np.log(mean/mean_weight)
        return result

    def compute(self):
        t = time.time()
        # 计算apb
        self.result = self.data_dropna.groupby(['s_info_windcode', 'trade_dt'])\
                                      .apply(self.oneday_apb)\
                                      .reset_index()\
                                      .rename(columns={0: 'apb1d'})
        print('compute using time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        # 数据输出
        item = ['trade_dt', 's_info_windcode', 'apb1d']
        self.result[item].to_pickle(self.save_indir + 'factor_hq_apb1d_' + self.file_name[17:21] + '.pkl')
        print('fileout using time:%10.4fs' % (time.time()-t))

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
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\minfactor\\'
    file_names = ['all_store_hqdata_2012_5_derive.pkl', 'all_store_hqdata_2013_5_derive.pkl',
                  'all_store_hqdata_2014_5_derive.pkl', 'all_store_hqdata_2015_5_derive.pkl',
                  'all_store_hqdata_2016_5_derive.pkl', 'all_store_hqdata_2017_5_derive.pkl',
                  'all_store_hqdata_2018_5_derive.pkl', 'all_store_hqdata_2019_5_derive.pkl']
    for file_name in file_names:
        print(file_name)
        apb = Apb(file_indir, save_indir, file_name)
        apb.runflow()

    # 分开数据读取、合并
    indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\minfactor\\'
    names = ['factor_hq_apb1d_2012.pkl', 'factor_hq_apb1d_2013.pkl',
             'factor_hq_apb1d_2014.pkl', 'factor_hq_apb1d_2015.pkl',
             'factor_hq_apb1d_2016.pkl', 'factor_hq_apb1d_2017.pkl',
             'factor_hq_apb1d_2018.pkl', 'factor_hq_apb1d_2019.pkl']
    data_sum = []
    for name in names:
        data_sum.append(pd.read_pickle(indir + name))
    temp_result = pd.concat(data_sum)

    # 合并数据对齐、输出
    all_data = pd.read_pickle(file_indir + 'all_dayindex.pkl')
    result = pd.merge(all_data[['trade_dt', 's_info_windcode']], temp_result, how='left')
    item = ['trade_dt', 's_info_windcode', 'apb1d']
    result[item].to_pickle('D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\factor_hq_apb1d.pkl')
