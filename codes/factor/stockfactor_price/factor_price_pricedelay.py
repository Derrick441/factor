import pandas as pd
import numpy as np
import time
import statsmodels.regression.rolling as regroll


# 日频率
# 价格时滞：市场收益率滞后项对股票收益率的解释
# 回归t0股票收益率与t0市场收益率，得到r_squared解释系数R1
# 回归t0股票收益率与t0、t-1、t-2和t-3市场收益率，得到r_squared解释系数R2
# 1-(R1/R2)
class PriceDelay(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 股票日行情数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        # zz500数据
        self.zz500 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\zz500_indexprice.pkl')
        self.zz500.reset_index(inplace=True)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 计算市场收益率及滞后项
        self.zz500['mkt'] = self.zz500['s_dq_change'] / self.zz500['s_dq_preclose'] * 100
        self.zz500['mkt1'] = self.zz500['mkt'].shift(1)
        self.zz500['mkt2'] = self.zz500['mkt'].shift(2)
        self.zz500['mkt3'] = self.zz500['mkt'].shift(3)
        # 汇总股票收益率和市场收益率
        self.data_sum = pd.merge(self.all_data[['trade_dt', 's_info_windcode', 'change']],
                                 self.zz500[['trade_dt', 'mkt', 'mkt1', 'mkt2', 'mkt3']],
                                 how='left')
        # 0和空值处理
        self.temp = self.data_sum.replace(0, np.nan).copy()
        self.data_dropna = self.temp.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def roll_regress(self, data, perid):
        t = time.time()
        temp_data = data.copy()
        num = len(data)
        if num > perid:
            temp_data['intercept'] = 1
            item1 = ['intercept', 'mkt']
            item2 = ['intercept', 'mkt', 'mkt1', 'mkt2', 'mkt3']
            model1 = regroll.RollingOLS(temp_data['change'], temp_data[item1], window=perid).fit()
            model2 = regroll.RollingOLS(temp_data['change'], temp_data[item2], window=perid).fit()
            temp = 1-(model1.rsquared / model2.rsquared)
            result = pd.DataFrame({'trade_dt': temp_data.trade_dt.values, 'pricedelay': temp.values})
            print(time.time() - t)
            return result
        else:
            result = pd.DataFrame({'trade_dt': temp_data.trade_dt.values, 'pricedelay': [None for i in range(num)]})
            return result

    def compute(self):
        t = time.time()
        # 每股滚动回归计算价格时滞
        self.temp_result = self.data_dropna.groupby(['s_info_windcode'])\
                                           .apply(self.roll_regress, 20)\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.all_data = pd.read_pickle(self.file_indir + 'all_dayindex.pkl')
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 输出到factor文件夹的stockfactor中
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
        print('end running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_dayindex.pkl'

    pricedelay = PriceDelay(file_indir, save_indir, file_name)
    pricedelay.runflow()
