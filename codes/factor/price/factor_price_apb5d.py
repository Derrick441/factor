import pandas as pd
import numpy as np
import time


# 5日均价偏差apb: 5日的成交量加权平均价的平均价/5日的成交量加权的成交量加权平均价的平均价
class Apb5d(object):

    def __init__(self, file_indir, file_name):
        self.file_indir = file_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 输入股价和成交量数据
        self.data = pd.read_pickle(self.file_indir[0] + self.file_name)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 将0值替换为nan
        self.data = self.data.replace(0, np.nan)
        # 去除没有成交量的数据
        self.data_drop = self.data[self.data['s_dq_volume'] != np.nan]
        # 日成交量加权平均价
        self.data_drop['vwap'] = self.data_drop['s_dq_amount'] / self.data_drop['s_dq_volume'] * 10
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def month_apb(self, data):
        temp = data.copy()
        # 计算5日的日成交量加权平均价的平均价
        temp['vwap_mean'] = temp['vwap'].rolling(5).mean()
        # 计算5日的成交量加权的日成交量加权平均价的平均价
        temp['volume_5'] = temp['s_dq_volume'].rolling(5).sum()
        temp['temp'] = temp['s_dq_volume'] / temp['volume_5'] * temp['vwap']
        temp['vwap_mean_vw'] = temp['temp'].rolling(5).sum()
        # 计算apb
        temp['apb5d'] = np.log(temp['vwap_mean'] / temp['vwap_mean_vw'])
        return temp[['trade_dt', 'apb5d']]

    def factor_compute(self):
        t = time.time()
        self.temp_result = self.data_drop.groupby('s_info_windcode')\
                                         .apply(self.month_apb)\
                                         .reset_index()
        print('factor_compute using time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', 'apb5d']
        self.result[item].to_pickle(self.file_indir[1] + 'factor_price_apb5d.pkl')
        print('fileout using time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.factor_compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = ['D:\\wuyq02\\develop\\python\\data\\developflow\\all\\',
                  'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\']
    file_name = 'all_band_price.pkl'
    apb = Apb5d(file_indir, file_name)
    apb.runflow()

# apb.filein()
# apb.datamanage()
# apb.factor_compute()
