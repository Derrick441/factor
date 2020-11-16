import pandas as pd
import numpy as np
import time
import statsmodels.regression.rolling as regroll


# 滚动计算：因子的时间序列回归newey-west调整标准误
class WhiteAdj(object):

    def __init__(self, file_indir, factor_indir, file_name, factor):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.file_name = file_name
        self.factor = factor
        print(self.file_name)

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        self.fac = pd.read_pickle(self.factor_indir + self.factor)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.fac_name = self.fac.columns[-1]
        print(self.fac_name)
        self.fac = self.fac.rename(columns={self.fac_name: 'factor'})

        item = ['trade_dt', 's_info_windcode', 's_dq_pctchange']
        self.data_sum = pd.merge(self.fac, self.all_data[item], how='left')
        self.data_dropna = self.data_sum.dropna().copy()
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def method(self, data, perid, factor_name):
        t = time.time()
        temp = data.copy()
        name = 'sdw' + factor_name
        num = len(temp)
        if num >= perid:
            temp['intercept'] = 1
            item = ['intercept', 'factor']
            model = regroll.RollingOLS(temp['s_dq_pctchange'], temp[item], window=perid).fit(cov_type='HC0')
            result = np.sqrt(model.cov_params()['factor'][1::2])
            print(time.time() - t)
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, name: result.values})
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
                                    .apply(self.method, 20, self.fac_name)\
                                    .reset_index()
        if self.fac_name in ['rvol', 'vvol']:
            self.mean = self.data_dropna.groupby('s_info_windcode')\
                                        .apply(self.rolling_mean, 20)\
                                        .reset_index()
            self.stde['sdw'+self.fac_name] = self.stde['sdw' + self.fac_name] / self.mean['temp_mean']
        print('compute using time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.stde, how='left')
        item = ['trade_dt', 's_info_windcode', 'sdw' + self.fac_name]
        self.result[item].to_pickle(self.factor_indir + 'factor_hq_sdw' + self.fac_name + '.pkl')
        print('fileout using time:%10.4fs' % (time.time()-t))

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
        wa = WhiteAdj(file_indir, factor_indir, file_name, factor)
        wa.runflow()
