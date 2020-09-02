import pandas as pd
import numpy as np
import time
import statsmodels.api as sm


# 日频率
# 高频数据计算的：日内特质波动率，日内偏度，日内峰度
class IntradayThressIndex(object):

    def __init__(self, file_indir, file_name):
        self.file_indir = file_indir
        self.file_name = file_name

    def filein(self, file):
        t = time.time()
        # 从dataflow文件夹中取日内5分钟高频数据
        self.data_5min = pd.read_pickle(self.file_indir + file)
        # 从factor文件夹中取5分钟因子数据
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\mktfactor\\'
        self.mkt_5min = pd.read_pickle(indir_factor + 'factor_mkt_5min_' + file[17:21] + '.pkl')
        self.smb_5min = pd.read_pickle(indir_factor + 'factor_smb_5min_' + file[17:21] + '.pkl')
        self.hml_5min = pd.read_pickle(indir_factor + 'factor_hml_5min_' + file[17:21] + '.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        self.data = pd.merge(self.data_5min, self.mkt_5min, how='left')
        self.data = pd.merge(self.data, self.smb_5min, how='left')
        self.data = pd.merge(self.data, self.hml_5min, how='left')
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def regress(self, data, y, x):
        Y = data[y]
        X = data[x]
        X['intercept'] = 1
        result = sm.OLS(Y, X).fit()
        return np.sqrt(result.ssr), result.resid.skew(), result.resid.kurt()

    def compute_intraday3index(self):
        t = time.time()
        temp = self.data.groupby(['s_info_windcode', 'trade_dt']) \
                        .apply(self.regress, 'change', ['mkt', 'smb', 'hml'])\
                        .apply(pd.Series)\
                        .reset_index()
        self.temp_result = temp.rename(columns={0: 'Dvol', 1: 'Dskew', 2: 'Dkurt'})
        print('compute_intraday3index running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.result_sum = []
        # 分年度计算因子
        for i in self.file_name:
            print(i)
            self.filein(i)
            self.data_manage()
            self.compute_intraday3index()
            self.result_sum.append(self.temp_result)

        # 因子汇总
        print('sum')
        self.result_concat = pd.concat(self.result_sum)
        # 数据对齐
        self.all_data = pd.read_pickle(self.file_indir + 'all_dayindex.pkl')
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.result_concat, how='left')

        # 三个因子分别输出到factor文件夹的stockfactor中
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        item1 = ['trade_dt', 's_info_windcode', 'Dvol']
        item2 = ['trade_dt', 's_info_windcode', 'Dskew']
        item3 = ['trade_dt', 's_info_windcode', 'Dkurt']
        self.result[item1].to_pickle(indir_factor + 'factor_price_Dvol.pkl')
        self.result[item2].to_pickle(indir_factor + 'factor_price_Dskew.pkl')
        self.result[item3].to_pickle(indir_factor + 'factor_price_Dkurt.pkl')
        print('end running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    # file_index = ['all_store_hqdata_2012_5_derive.pkl']
    file_name = ['all_store_hqdata_2012_5_derive.pkl', 'all_store_hqdata_2013_5_derive.pkl',
                 'all_store_hqdata_2014_5_derive.pkl', 'all_store_hqdata_2015_5_derive.pkl',
                 'all_store_hqdata_2016_5_derive.pkl', 'all_store_hqdata_2017_5_derive.pkl',
                 'all_store_hqdata_2018_5_derive.pkl', 'all_store_hqdata_2019_5_derive.pkl']
    df = IntradayThressIndex(file_indir, file_name)
    df.runflow()
