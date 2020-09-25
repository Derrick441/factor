import pandas as pd
import numpy as np
import time


# 收益的动量成分和反转成分：动量成分=对数（隔夜收益+日内温和收益之和），反转成分=对数（日内极端收益之和）
class MomRev(object):

    def __init__(self, file_indirs, file_names):
        self.file_indirs = file_indirs
        self.file_names = file_names

    def filein(self, indir, name):
        t = time.time()
        # 输入股价和成交量数据
        self.data = pd.read_pickle(indir + name)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 将0值替换为nan
        self.data = self.data.replace(0, np.nan)
        # 去除没有收盘价的数据
        self.data_drop = self.data[self.data['closeprice'] != np.nan]
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def decompose(self, data):
        temp = data.copy()
        median = temp['closeprice'].median()
        temp['temp'] = temp['closeprice'].apply(lambda x: abs(x-median))
        MAD = temp['temp'].median()
        MADe = 1.483*MAD
        temp['flag'] = temp['closeprice'].apply(lambda x: 1 if ((x-median > MADe) | (x-median < -MADe)) else 0)
        mild = (temp['change'][temp['flag'] == 0]).sum()
        extreme = (temp['change'][temp['flag'] == 1]).sum()
        return mild, extreme

    def factor_compute(self):
        t = time.time()
        # 计算arpp
        self.result_part = self.data_drop.groupby(['s_info_windcode', 'trade_dt'])\
                                         .apply(self.decompose)\
                                         .apply(pd.Series) \
                                         .reset_index() \
                                         .rename(columns={0: 'mild', 1: 'extreme'})
        print('factor_compute using time:%10.4fs' % (time.time()-t))

    def overnight(self, data):
        temp = data.copy()
        temp['temp_o_n_r'] = (temp['s_dq_close'] - temp['s_dq_open'].shift(1))
        temp['o_n_r'] = (temp['temp_o_n_r'] / temp['s_dq_close'].shift(1)) * 100
        return temp['o_n_r']

    def runflow(self):
        t = time.time()
        print('start')
        # 温和收益和极端收益
        self.result_sum = []
        for i in self.file_names:
            print(i)
            self.filein(self.file_indirs[0], i)
            self.datamanage()
            self.factor_compute()
            self.result_sum.append(self.result_part)
        self.temp_result = pd.concat(self.result_sum)
        # 隔夜收益
        self.all_data = pd.read_pickle(self.file_indirs[0] + 'all_band_price.pkl')
        self.overnight_return = self.all_data.groupby('s_info_windcode')\
                                             .apply(self.overnight)\
                                             .reset_index()
        # 数据对齐
        self.result1 = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.overnight_return, how='left')
        self.result = pd.merge(self.result1, self.temp_result, how='left')
        # 数据输出
        self.result['momr'] = np.log(self.result['o_n_r'] + self.result['mild'])
        self.result['revr'] = np.log(self.result['extreme'])
        item = ['trade_dt', 's_info_windcode', 'momr']

        self.result[item].to_pickle(self.file_indirs[1] + 'factor_hq_momr.pkl')
        item1 = ['trade_dt', 's_info_windcode', 'revr']
        self.result[item1].to_pickle(self.file_indirs[1] + 'factor_hq_revr.pkl')
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = ['D:\\wuyq02\\develop\\python\\data\\developflow\\all\\',
                  'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\']
    file_name = ['all_store_hqdata_2012_5_derive.pkl', 'all_store_hqdata_2013_5_derive.pkl',
                 'all_store_hqdata_2014_5_derive.pkl', 'all_store_hqdata_2015_5_derive.pkl',
                 'all_store_hqdata_2016_5_derive.pkl', 'all_store_hqdata_2017_5_derive.pkl',
                 'all_store_hqdata_2018_5_derive.pkl', 'all_store_hqdata_2019_5_derive.pkl']
    mr = MomRev(file_indir, file_name)
    mr.runflow()


# data = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\all\\' + 'all_store_hqdata_2012_5_derive.pkl')
#
# data_temp = data[data.s_info_windcode == '000001.SZ']
#
#
# def decompose(data):
#     temp = data.copy()
#     median = temp['closeprice'].median()
#     temp['temp'] = temp['closeprice'].apply(lambda x: abs(x - median))
#     MAD = temp['temp'].median()
#     MADe = 1.483 * MAD
#     temp['flag'] = temp['closeprice'].apply(lambda x: 1 if ((x - median > MADe) | (x - median < -MADe)) else 0)
#     mild = (temp['change'][temp['flag'] == 0]).sum()
#     extreme = (temp['change'][temp['flag'] == 1]).sum()
#     return mild, extreme
#
#
# result_part = data_temp.groupby(['s_info_windcode', 'trade_dt'])\
#                                 .apply(decompose)\
#                                 .apply(pd.Series) \
#                                 .reset_index() \
#                                 .rename(columns={0: 'mild', 1: 'extreme'})


# all_data = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\all\\' + 'all_band_price.pkl')
#
# data_temp = all_data[(all_data.s_info_windcode == '000001.SZ') | (all_data.s_info_windcode == '000006.SZ')]
#
#
# def overnight(data):
#     temp = data.copy()
#     temp['temp_o_n_r'] = (temp['s_dq_close'] - temp['s_dq_open'].shift(1))
#     temp['o_n_r'] = (temp['temp_o_n_r'] / temp['s_dq_close'].shift(1)) * 100
#     return temp['o_n_r']
#
#
# overnight_return = data_temp.groupby('s_info_windcode') \
#                             .apply(overnight) \
#                             .reset_index()
