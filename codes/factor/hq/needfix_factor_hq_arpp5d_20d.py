import pandas as pd
import numpy as np
import time


# arpp时间加权平均的相对价格位置: （p-l)/(h-l)在时间上的积分
# 若交易时间合计为1，则（p-l)/(h-l)*delta t等于（p-l)/(h-l)*1/n，n为微小单位的数量
# （p-l)/(h-l)在时间上的积分等价于n个（p-l)/(h-l)的平均值
class Arpp(object):

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

    def dayin_twaplh(self, data):
        temp = data.copy()
        # 日内最高最低价
        L = temp['lowprice'].min()
        H = temp['highprice'].max()
        # 1分钟开盘收盘价均值
        temp['ocmean'] = (temp['openprice'] + temp['closeprice'])/2
        # 计算twap
        twap = temp['ocmean'].mean()
        return twap, L, H

    def roll_arpp5d(self, data):
        temp = data.copy()
        if len(data) > 5:
            result = [None for i in range(5)]
            for i in range(5, len(temp)):
                temp5d = temp.iloc[i-5:i, :]
                twap = temp5d.twap.mean()
                L = temp5d.L.min()
                H = temp5d.H.max()
                if (H-L) == 0:
                    result.append(np.nan)
                else:
                    result.append((twap-L)/(H-L))
            return pd.DataFrame({'trade_dt': temp.trade_dt, 'arpp5d': result})
        else:
            result = [None for i in range(len(data))]
            return pd.DataFrame({'trade_dt': temp.trade_dt, 'arpp5d': result})

    def roll_arpp20d(self, data):
        temp = data.copy()
        if len(data) > 20:
            result = [None for i in range(20)]
            for i in range(20, len(temp)):
                temp20d = temp.iloc[i-20:i, :]
                twap = temp20d.twap.mean()
                L = temp20d.L.min()
                H = temp20d.H.max()
                if (H - L) == 0:
                    result.append(np.nan)
                else:
                    result.append((twap - L) / (H - L))
            return pd.DataFrame({'trade_dt': temp.trade_dt, 'arpp20d': result})
        else:
            result = [None for i in range(len(data))]
            return pd.DataFrame({'trade_dt': temp.trade_dt, 'arpp20d': result})

    def factor_compute(self):
        t = time.time()
        # 计算arpp
        self.data_twapLH = self.data_drop.groupby(['s_info_windcode', 'trade_dt'])\
                                         .apply(self.dayin_twaplh)\
                                         .apply(pd.Series) \
                                         .reset_index() \
                                         .rename(columns={0: 'twap', 1: 'L', 2: 'H'})
        self.result_part5d = self.data_twapLH.groupby('s_info_windcode')\
                                             .apply(self.roll_arpp5d)\
                                             .reset_index()
        self.result_part20d = self.data_twapLH.groupby('s_info_windcode')\
                                              .apply(self.roll_arpp20d)\
                                              .reset_index()
        print('factor_compute using time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.result_sum5d = []
        self.result_sum20d = []
        for i in self.file_names:
            print(i)
            self.filein(self.file_indirs[0], i)
            self.datamanage()
            self.factor_compute()
            self.result_sum5d.append(self.result_part5d)
            self.result_sum20d.append(self.result_part20d)
        self.temp_result5d = pd.concat(self.result_sum5d)
        self.temp_result20d = pd.concat(self.result_sum20d)
        # 数据对齐
        self.all_data = pd.read_pickle(self.file_indirs[0] + 'all_dayindex.pkl')
        self.result5d = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result5d, how='left')
        self.result20d = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result20d, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', 'arpp20d']
        self.result5d[item].to_pickle(self.file_indirs[1] + 'factor_hq_arpp5d.pkl')
        self.result20d[item].to_pickle(self.file_indirs[1] + 'factor_hq_arpp20d.pkl')
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = ['D:\\wuyq02\\develop\\python\\data\\developflow\\all\\',
                  'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\']
    file_name = ['all_store_hqdata_2012.pkl', 'all_store_hqdata_2013.pkl',
                 'all_store_hqdata_2014.pkl', 'all_store_hqdata_2015.pkl',
                 'all_store_hqdata_2016.pkl', 'all_store_hqdata_2017.pkl',
                 'all_store_hqdata_2018.pkl', 'all_store_hqdata_2019.pkl']
    arpp = Arpp(file_indir, file_name)
    arpp.runflow()

# arpp.filein(file_indir[0], file_name[0])
# arpp.datamanage()
# arpp.factor_compute()

# data = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\all\\' + 'all_store_hqdata_2012.pkl')
#
# temp_data = data.iloc[0:100000,:]
#
# def dayin_twapLH(data):
#     temp = data.copy()
#     # 日内最高最低价
#     L = temp['lowprice'].min()
#     H = temp['highprice'].max()
#     # 1分钟开盘收盘价均值
#     temp['ocmean'] = (temp['openprice'] + temp['closeprice'])/2
#     # 计算twap
#     twap = temp['ocmean'].mean()
#     return twap, L, H
#
# data_twapLH = temp_data.groupby(['s_info_windcode', 'trade_dt'])\
#                        .apply(dayin_twapLH)\
#                        .apply(pd.Series)\
#                        .reset_index()\
#                        .rename(columns={0: 'twap', 1: 'L', 2: 'H'})
#
# def roll_arpp(data):
#     temp = data.copy()
#     result = [None for i in range(5)]
#     for i in range(5, len(temp)):
#         temp5d = temp.iloc[i - 5:i, :]
#         twap = temp5d.twap.mean()
#         L = temp5d.L.min()
#         H = temp5d.H.max()
#         result.append((twap - L) / (H - L))
#     return pd.DataFrame({'trade_dt': temp.trade_dt, 'arpp': result})
#
#
# result_part = data_twapLH.groupby('s_info_windcode') \
#     .apply(roll_arpp) \
#     .reset_index()
