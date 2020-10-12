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
        # 股票日行情数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        # zz500数据
        self.zz500 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\zz500_indexprice.pkl')
        self.zz500.reset_index(inplace=True)
        print('filein running time:%10.4fs' % (time.time()-t))

    def daytrade_factor(self, data, index):
        num = int(len(data) / 3)
        # 取当日流通市值/估值最小1/3股票的收益率均值
        low = data.sort_values(by=index).iloc[:num, :].change.mean()
        # 取当日流通市值/估值最大1/3股票的收益率均值
        high = data.sort_values(by=index).iloc[-num:, :].change.mean()
        # 当日市值因子
        return low - high

    def datamanage(self):
        t = time.time()
        temp_data = self.all_data[['trade_dt', 's_info_windcode', 'change']].copy()
        # 计算每日市场因子
        self.zz500['mkt'] = self.zz500['s_dq_change'] / self.zz500['s_dq_preclose'] * 100
        self.data_sum = pd.merge(temp_data, self.zz500[['trade_dt', 'mkt']], how='left')
        # 计算每日市值因子
        self.all_smb = self.all_data.groupby('trade_dt')\
                                    .apply(self.daytrade_factor, 's_dq_mv')\
                                    .reset_index()\
                                    .rename(columns={0: 'smb'})
        self.data_sum = pd.merge(self.data_sum, self.all_smb[['trade_dt', 'smb']], how='left')
        # 计算每日估值因子
        self.all_hml = self.all_data.groupby('trade_dt')\
                                    .apply(self.daytrade_factor, 's_val_pe_ttm')\
                                    .reset_index()\
                                    .rename(columns={0: 'hml'})
        self.data_sum = pd.merge(self.data_sum, self.all_hml[['trade_dt', 'hml']], how='left')
        # 0和空值处理
        self.temp = self.data_sum.replace(0, np.nan).copy()
        self.data_dropna = self.temp.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def rolling_regress(self, data, perid):
        t = time.time()
        temp_data = data.copy()
        num = len(data)
        if num > perid:
            temp_data['intercept'] = 1
            item = ['intercept', 'mkt', 'smb', 'hml']
            model = regroll.RollingOLS(temp_data['change'], temp_data[item], window=perid).fit()
            temp = np.sqrt(model.mse_resid)*np.sqrt(243)
            result = pd.DataFrame({'trade_dt': temp_data.trade_dt.values, 'ivff': temp.values})
            print(time.time() - t)
            return result
        else:
            result = pd.DataFrame({'trade_dt': temp_data.trade_dt.values, 'ivff': [None for i in range(len(data))]})
            return result

    def compute(self):
        t = time.time()
        # 每股滚动回归计算ivff
        self.temp_result = self.data_dropna.groupby('s_info_windcode')\
                                           .apply(self.rolling_regress, 20)\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.all_data = pd.read_pickle(self.file_indir + 'all_dayindex.pkl')
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 输出到factor文件夹的stockfactor中
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
        print('end running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_dayindex.pkl'

    ivff = Ivff(file_indir, save_indir, file_name)
    ivff.runflow()
