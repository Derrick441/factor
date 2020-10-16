import pandas as pd
import numpy as np
import time
import statsmodels.api as sm


# 滚动计算：因子的时间序列回归newey-west调整标准误
class NeweyWestAdj(object):

    def __init__(self, file_indir, factor_indir, file_name, factor):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.file_name = file_name
        self.factor = factor

    def filein(self):
        t = time.time()
        # 股票日数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        # 7因子数据
        self.fac = pd.read_pickle(self.factor_indir + self.factor)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 因子名保留、数据因子名调整
        self.fac_name = self.fac.columns[-1]
        print(self.fac_name)
        self.fac = self.fac.rename(columns={self.fac_name: 'factor'})

        # 因子和收益率合并
        item = ['trade_dt', 's_info_windcode', 'change']
        self.data_sum = pd.merge(self.fac, self.all_data[item], how='left')
        # 去除nan
        self.data_dropna = self.data_sum.dropna().copy()
        print('datamanage using time:%10.4fs' % (time.time()-t))

    # ******************************************************************************************************************
    def neweywest_stde(self, data, perid, factor_name):
        t = time.time()
        temp = data.copy()
        name = 'sd' + factor_name
        num = len(temp)
        if num > perid:
            result = [None for i in range(perid)]
            temp['intercept'] = 1
            item = ['intercept', 'factor']
            # 第21个开始，计算回归的newey-west调整标准误
            for i in range(perid, num):
                temp1 = temp.iloc[(i-perid):i, :]
                model = sm.OLS(temp1['change'], temp1[item]).fit(cov_type='HAC', cov_kwds={'maxlags': 3})
                stde = model.params / model.tvalues
                result.append(stde.values[1])
            print(time.time()-t)
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, name: result})
        else:
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, name: [None for i in range(num)]})

    def rolling_mean(self, data, perid):
        temp = data.copy()
        num = len(temp)
        if num > perid:
            result = temp['factor'].rolling(perid).mean()
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, 'temp_mean': result})
        else:
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, 'temp_mean': [None for i in range(num)]})

    def compute(self):
        t = time.time()
        self.stde = self.data_dropna.groupby('s_info_windcode')\
                                    .apply(self.neweywest_stde, 20, self.fac_name)\
                                    .reset_index()
        if self.fac_name in ['rvol', 'vvol']:
            self.mean = self.data_dropna.groupby('s_info_windcode')\
                                        .apply(self.rolling_mean, 20)\
                                        .reset_index()
            self.stde['sd'+self.fac_name] = self.stde['sd' + self.fac_name] / self.mean['temp_mean']
        print('factor compute using time:%10.4fs' % (time.time()-t))
    # ******************************************************************************************************************

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.stde, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', 'sd' + self.fac_name]
        self.result[item].to_pickle(self.factor_indir + 'factor_hq_sd' + self.fac_name + '.pkl')
        print(time.time()-t)

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_dayindex.pkl'
    factors = ['factor_hq_rskew.pkl', 'factor_hq_rkurt.pkl',
               'factor_hq_rvol.pkl',  'factor_hq_vskew.pkl',
               'factor_hq_vkurt.pkl', 'factor_hq_vvol.pkl',
               'factor_hq_vhhi.pkl']

    for factor in factors:
        # ******************************************************************************************************************
        nw = NeweyWestAdj(file_indir, factor_indir, file_name, factor)
        nw.runflow()
        # ******************************************************************************************************************
