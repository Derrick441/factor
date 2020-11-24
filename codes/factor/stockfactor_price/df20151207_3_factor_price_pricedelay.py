import pandas as pd
import numpy as np
import time
import statsmodels.regression.rolling as regroll


# 价格时滞：市场收益率对股票收益率的解释
# t0股票收益率与t0市场收益率回归，得到解释系数R1
# t0股票收益率与t0、t-1、t-2和t-3市场收益率回归，得到解释系数R2
# (R2-R1)/R2) = 1-(R1/R2)
class PriceDelay(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        self.zz500 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\zz500_indexprice.pkl')
        self.zz500.reset_index(inplace=True)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.zz500['mkt'] = self.zz500['s_dq_change'] / self.zz500['s_dq_preclose'] * 100
        self.zz500['mkt1'] = self.zz500['mkt'].shift(1)
        self.zz500['mkt2'] = self.zz500['mkt'].shift(2)
        self.zz500['mkt3'] = self.zz500['mkt'].shift(3)
        item1 = ['trade_dt', 's_info_windcode', 's_dq_pctchange']
        item2 = ['trade_dt', 'mkt', 'mkt1', 'mkt2', 'mkt3']
        self.data = pd.merge(self.all_data[item1], self.zz500[item2], how='left')
        self.data_dropna = self.data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def method(self, data, perid):
        t = time.time()
        temp = data.copy()
        num = len(data)
        if num >= perid:
            temp['intercept'] = 1
            item1 = ['intercept', 'mkt']
            model1 = regroll.RollingOLS(temp['s_dq_pctchange'], temp[item1], window=perid).fit()
            item2 = ['intercept', 'mkt', 'mkt1', 'mkt2', 'mkt3']
            model2 = regroll.RollingOLS(temp['s_dq_pctchange'], temp[item2], window=perid).fit()
            temp_result = 1-(model1.rsquared / model2.rsquared)
            print(time.time() - t)
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, 'pricedelay': temp_result.values})
        else:
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, 'pricedelay': [None for i in range(num)]})

    def compute(self):
        t = time.time()
        self.temp_result = self.data_dropna.groupby(['s_info_windcode'])\
                                           .apply(self.method, 20)\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', 'pricedelay']
        self.result[item].to_pickle(self.save_indir + 'factor_price_pricedelay.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

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

    pricedelay = PriceDelay(file_indir, save_indir, file_name)
    pricedelay.runflow()