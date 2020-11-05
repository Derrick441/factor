import pandas as pd
import numpy as np
import time
import statsmodels.regression.rolling as regroll


# 特质波动率
class Ivff(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        self.zz500 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\zz500_indexprice.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def daytrade_factor(self, data, index):
        temp = data.copy()
        num = int(len(temp) / 3)
        low = temp.sort_values(by=index).iloc[:num, :].s_dq_pctchange.mean()
        high = temp.sort_values(by=index).iloc[-num:, :].s_dq_pctchange.mean()
        return low - high

    def datamanage(self):
        t = time.time()
        temp_data = self.all_data[['trade_dt', 's_info_windcode', 's_dq_pctchange']].copy()
        # 计算每日市场因子
        self.zz500.reset_index(inplace=True)
        self.zz500['mkt'] = self.zz500['s_dq_change'] / self.zz500['s_dq_preclose'] * 100
        self.data_sum = pd.merge(temp_data, self.zz500[['trade_dt', 'mkt']], how='left')
        # 计算每日市值因子
        self.all_smb = self.all_data.groupby('trade_dt')\
                                    .apply(self.daytrade_factor, 's_dq_freemv')\
                                    .reset_index()\
                                    .rename(columns={0: 'smb'})
        self.data_sum = pd.merge(self.data_sum, self.all_smb[['trade_dt', 'smb']], how='left')
        # 计算每日估值因子
        self.all_hml = self.all_data.groupby('trade_dt')\
                                    .apply(self.daytrade_factor, 's_val_pb_new')\
                                    .reset_index()\
                                    .rename(columns={0: 'hml'})
        self.data_sum = pd.merge(self.data_sum, self.all_hml[['trade_dt', 'hml']], how='left')
        self.data_dropna = self.data_sum.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def method(self, data, perid):
        t = time.time()
        temp = data.copy()
        num = len(data)
        if num >= perid:
            temp['intercept'] = 1
            item = ['intercept', 'mkt', 'smb', 'hml']
            model = regroll.RollingOLS(temp['s_dq_pctchange'], temp[item], window=perid).fit()
            temp_result = np.sqrt(model.mse_resid)*np.sqrt(243)
            print(time.time() - t)
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, 'ivff': temp_result.values})
        else:
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, 'ivff': [None for i in range(num)]})

    def compute(self):
        t = time.time()
        self.temp_result = self.data_dropna.groupby('s_info_windcode')\
                                           .apply(self.method, 20)\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', 'ivff']
        self.result[item].to_pickle(self.save_indir + 'factor_price_ivff.pkl')
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

    ivff = Ivff(file_indir, save_indir, file_name)
    ivff.runflow()
