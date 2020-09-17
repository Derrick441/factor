import pandas as pd
import numpy as np
import time


# 均价偏差apb: 20日平均价/20日成交量加权平均价
class Apb(object):

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
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def factor_compute(self):
        t = time.time()
        # 计算20日平均价
        self.data['vwap'] = self.data['s_dq_amount'] / self.data['s_dq_volume']*10
        self.data['vwap_mean'] = self.data['vwap'].rolling(20).mean()
        # 计算20日成交量加权平均价
        self.data['volume_20'] = self.data['s_dq_volume'].rolling(20).sum()
        self.data['temp'] = self.data['vwap'] * self.data['s_dq_volume'] / self.data['volume_20']
        self.data['vwap_vwmean'] = self.data['temp'].rolling(20).sum()
        # 计算apb
        self.data['apb'] = self.data['vwap_mean'] / self.data['vwap_vwmean']
        print('factor_compute using time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        item = ['trade_dt', 's_info_windcode', 'apb']
        self.data[item].to_pickle(self.file_indir[1] + 'factor_price_apb.pkl')
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
    apb = Apb(file_indir, file_name)
    apb.runflow()

# apb.filein()
# apb.datamanage()
# apb.factor_compute()
#
# # 计算20日平均价
# apb.data['vwap'] = apb.data['s_dq_amount'] / apb.data['s_dq_volume']*10
# apb.data['vwap_mean'] = apb.data['vwap'].rolling(20).mean()
# # 计算20日成交量加权平均价
# apb.data['volume_20'] = apb.data['s_dq_volume'].rolling(20).sum()
# apb.data['temp'] = apb.data['vwap'] * apb.data['s_dq_volume'] / apb.data['volume_20']
# apb.data['vwap_vwmean'] = apb.data['temp'].rolling(20).sum()
# # 计算apb
# apb.data['apb'] = apb.data['vwap_mean'] / apb.data['vwap_vwmean']

# x = pd.read_pickle(file_indir[1]+'factor_price_apb.pkl')
