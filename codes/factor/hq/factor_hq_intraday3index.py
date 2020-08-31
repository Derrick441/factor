import pandas as pd
import numpy as np
import time
import statsmodels.api as sm


# 日频率
# 高频数据计算的：日内特质波动率，日内偏度，日内峰度
class IntradayThressIndex(object):

    def __init__(self, file_indir, file_f):
        self.file_indir = file_indir
        self.file_f = file_f

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中取日内5分钟高频数据
        self.data_5min = pd.read_pickle(self.file_indir + self.file_f)
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        # t = time.time()
        # print('datamanage running time:%10.4fs' % (time.time() - t))
        pass

    def regress1(self, data, y, x):
        Y = data[y]
        X = data[x]
        X['intercept'] = 1
        result = sm.OLS(Y, X).fit()
        return np.sqrt(result.ssr)

    def regress2(self, data, y, x):
        Y = data[y]
        X = data[x]
        X['intercept'] = 1
        result = sm.OLS(Y, X).fit()
        return result.resid.skew()

    def regress3(self, data, y, x):
        Y = data[y]
        X = data[x]
        X['intercept'] = 1
        result = sm.OLS(Y, X).fit()
        return result.resid.kurt()

    def compute_intraday3index(self):
        t = time.time()
        # 每股滚动回归计算ivff
        self.result = self.data_5min.groupby(['s_info_windcode', 'trade_dt']).apply(self.regress1,).reset_index()
        self.result.rename({0: 'Dvol'})
        self.result['DSkew'] = self.data_5min.groupby(['s_info_windcode', 'trade_dt']).apply(self.regress1).values
        self.result['DKurt'] = self.data_5min.groupby(['s_info_windcode', 'trade_dt']).apply(self.regress2).values
        print('compute_intraday3index running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'ivff']
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        self.result[item].to_pickle(indir_factor + 'factor_price_ivff.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('compute start')
        self.filein()
        self.data_manage()
        self.compute_ivff()
        self.fileout()
        print('compute finish, all running time:%10.4fs' % (time.time() - t))
        return self


if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_five = ['hpdata_5min_2012.pkl']
    # file_five = ['hpdata_5min_2012.pkl', 'hpdata_5min_2013.pkl', 'hpdata_5min_2014.pkl',
    #              'hpdata_5min_2015.pkl', 'hpdata_5min_2016.pkl', 'hpdata_5min_2017.pkl',
    #              'hpdata_5min_2018.pkl', 'hpdata_5min_2019.pkl', 'hpdata_5min_2020.pkl']
    for i in file_five:
        print(i[-8:-4])
        df = IntradayThressIndex(indir, i)
        df.runflow()
