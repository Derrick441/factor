import pandas as pd
import numpy as np
import time
import statsmodels.api as sm


# 高频数据计算的：日内特质波动率，日内偏度，日内峰度
class DayinThreeIndex(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 5分钟股票数据
        self.data_5min = pd.read_pickle(self.file_indir + self.file_name)
        # 5分钟因子数据
        self.mkt_5min = pd.read_pickle(self.factor_indir + 'factor_mkt_5min_' + self.file_name[17:21] + '.pkl')
        self.smb_5min = pd.read_pickle(self.factor_indir + 'factor_smb_5min_' + self.file_name[17:21] + '.pkl')
        self.hml_5min = pd.read_pickle(self.factor_indir + 'factor_hml_5min_' + self.file_name[17:21] + '.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 数据合并
        self.data = pd.merge(self.data_5min, self.mkt_5min, how='left')
        self.data = pd.merge(self.data, self.smb_5min, how='left')
        self.data = pd.merge(self.data, self.hml_5min, how='left')
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def regress(self, data, y, x):
        Y = data[y]
        X = data[x]
        X['intercept'] = 1
        result = sm.OLS(Y, X).fit()
        # 注：研报中日内特质波动率用的是ssr的开方，而非之前的std
        return np.sqrt(result.ssr), result.resid.skew(), result.resid.kurt()

    def compute(self):
        t = time.time()
        self.result = self.data.groupby(['s_info_windcode', 'trade_dt']) \
                               .apply(self.regress, 'change', ['mkt', 'smb', 'hml'])\
                               .apply(pd.Series)\
                               .reset_index()\
                               .rename(columns={0: 'Dvol', 1: 'Dskew', 2: 'Dkurt'})
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据输出
        item1 = ['trade_dt', 's_info_windcode', 'Dvol']
        self.result[item1].to_pickle(self.save_indir + 'factor_hq_Dvol_' + self.file_name[17:21] + '.pkl')
        item2 = ['trade_dt', 's_info_windcode', 'Dskew']
        self.result[item2].to_pickle(self.save_indir + 'factor_hq_Dskew_' + self.file_name[17:21] + '.pkl')
        item3 = ['trade_dt', 's_info_windcode', 'Dkurt']
        self.result[item3].to_pickle(self.save_indir + 'factor_hq_Dkurt_' + self.file_name[17:21] + '.pkl')
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
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\mktfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\minfactor\\'
    file_names = ['all_store_hqdata_2012_5_derive.pkl', 'all_store_hqdata_2013_5_derive.pkl',
                  'all_store_hqdata_2014_5_derive.pkl', 'all_store_hqdata_2015_5_derive.pkl',
                  'all_store_hqdata_2016_5_derive.pkl', 'all_store_hqdata_2017_5_derive.pkl',
                  'all_store_hqdata_2018_5_derive.pkl', 'all_store_hqdata_2019_5_derive.pkl']
    for file_name in file_names:
        print(file_name)
        dti = DayinThreeIndex(file_indir, factor_indir, save_indir, file_name)
        dti.runflow()

    def merge_data(factor_name, names):
        # 分开数据读取、合并
        indir1 = 'D:\\wuyq02\\develop\\python\\data\\factor\\minfactor\\'
        data_sum = []
        for name in names:
            data_sum.append(pd.read_pickle(indir1 + name))
        temp_result = pd.concat(data_sum)

        # 合并数据对齐、输出
        all_data = pd.read_pickle(file_indir + 'all_dayindex.pkl')
        result = pd.merge(all_data[['trade_dt', 's_info_windcode']], temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', factor_name]
        indir2 = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        result[item].to_pickle(indir2 + 'factor_hq_' + factor_name + '.pkl')

    factor_name = 'Dvol'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'Dskew'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'Dkurt'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)
