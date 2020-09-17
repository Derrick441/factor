import pandas as pd
import numpy as np
import time
import statsmodels.api as sm
import statsmodels.regression.rolling as regroll


# 计算滚动回归的newey-west调整标准误
class NeweyWestAdj(object):

    def __init__(self, file_indir, file_name):
        self.file_indir = file_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 读入收益率数据
        self.ret = pd.read_pickle(self.file_indir[0] + self.file_name[0])
        self.ret = self.ret.reset_index().rename(columns={0: 'ret'})
        # 读入因子数据
        self.fac = pd.read_pickle(self.file_indir[1] + self.file_name[1])
        self.fac_name = self.fac.columns[-1]
        self.fac = self.fac.rename(columns={self.fac_name: 'factor'})
        print('filein using time:%10.4fs' % (time.time()-t))
        print(self.fac_name)

    def datamanage(self):
        t = time.time()
        self.fac_ret = pd.merge(self.fac, self.ret, how='left')
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def neweywest_stde(self, data):
        t = time.time()
        print(len(data))
        if len(data) >= 20:
            temp = data.copy()
            temp['intercept'] = 1
            item = ['intercept', 'factor']
            result = [None for i in range(19)]
            for i in range(19, len(data)):
                # print(i)
                temp1 = temp.iloc[(i-19):(i+1), :]
                model = sm.OLS(temp1['ret'], temp1[item]).fit(cov_type='HAC', cov_kwds={'maxlags': 3})
                stde = model.params / model.tvalues
                result.append(stde.values[1])
            print(time.time()-t)
            return pd.DataFrame({'trade_dt': data.trade_dt.values, 'sd'+self.fac_name: result})
        else:
            result = [None for i in range(len(data))]
            return pd.DataFrame({'trade_dt': data.trade_dt.values, 'sd'+self.fac_name: result})

    def rollingmean(self, data):
        if len(data) >= 20:
            mean = data['factor'].rolling(20).mean()
            return pd.DataFrame({'trade_dt': data.trade_dt.values, 'mean': mean.vlaues})
        else:
            result = [None for i in range(len(data))]
            return pd.DataFrame({'trade_dt': data.trade_dt.values, 'mean': result})

    def factor_compute(self):
        t = time.time()
        self.fac_ret['temp'] = self.fac_ret.factor+self.fac_ret.ret
        self.fac_ret_drop = self.fac_ret[(self.fac_ret.temp > 0) | (self.fac_ret.temp < 0)].copy()
        self.stde = self.fac_ret_drop.groupby('s_info_windcode')\
                                     .apply(self.neweywest_stde)\
                                     .reset_index()\
                                     .drop('level_1', axis=1)
        if self.fac_name in ['rvol', 'vvol']:
            self.mean = self.fac_ret_drop.groupby('s_info_windcode')\
                                         .apply(self.rollingmean)\
                                         .reset_index()
            self.stde['sd'+self.fac_name] = self.stde['sd'+self.fac_name] / self.mean.mean
        print('factor compute using time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.fac[['trade_dt', 's_info_windcode']], self.stde, how='left')
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'sd'+self.fac_name]
        self.result[item].to_pickle(self.file_indir[1] + self.file_name[1][:-8] + 'sd' + self.file_name[1][-8:])
        print(time.time()-t)

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
    # file_name = ['all_band_adjvwap_hh_price_label1.pkl',
    #              'factor_hq_rskew.pkl']

    factor = ['factor_hq_rskew.pkl', 'factor_hq_rkurt.pkl', 'factor_hq_vhhi.pkl',
              'factor_hq_vkurt.pkl', 'factor_hq_vkurt.pkl',
              'factor_hq_rvol.pkl', 'factor_hq_vvol.pkl']

    for i in factor:
        file_name = ['all_band_adjvwap_hh_price_label1.pkl', i]
        nw = NeweyWestAdj(file_indir, file_name)
        nw.runflow()


# nw.filein()
# nw.datamanage()
# nw.factor_compute()
#
#     def neweywest_stde(data):
#         t = time.time()
#         print(len(data))
#         if len(data) >= 20:
#             temp = data.copy()
#             temp['intercept'] = 1
#             item = ['intercept', 'factor']
#             result = [None for i in range(19)]
#             for i in range(19, len(data)):
#                 temp1 = temp.iloc[(i-19):(i+1), :]
#                 model = sm.OLS(temp1['ret'], temp1[item]).fit(cov_type='HAC', cov_kwds={'maxlags': 3})
#                 stde = model.params / model.tvalues
#                 result.append(stde.values[1])
#             print(time.time()-t)
#             return pd.DataFrame({'trade_dt': data.trade_dt.values, 'sd': result})
#         else:
#             result = [None for i in range(len(data))]
#             return pd.DataFrame({'trade_dt': data.trade_dt.values, 'sd': result})
#
# temp = nw.fac_ret.iloc[:2800000, :]
# fac_ret_dropna = temp.dropna()
# result = fac_ret_dropna.groupby('s_info_windcode').apply(neweywest_stde).reset_index().drop('level_1', axis=1)
# result
