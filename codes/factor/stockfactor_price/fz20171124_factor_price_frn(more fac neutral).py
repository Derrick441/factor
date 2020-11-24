import pandas as pd
import numpy as np
import time
import statsmodels.api as sm


# 因子中性化
class FactorNeutral(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_names, factor_names, factor_name):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_names = file_names
        self.factor_names = factor_names
        self.factor_name = factor_name
        print(self.factor_name)

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir + self.file_names[0])
        self.bandindu = pd.read_pickle(self.file_indir + self.file_names[1])
        self.mom20 = pd.read_pickle(self.factor_indir + self.factor_names[0])
        self.std60 = pd.read_pickle(self.factor_indir + self.factor_names[1])
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.fac_name = self.factor.columns[-1]

        self.mv = self.all_data[['trade_dt', 's_info_windcode', 's_dq_freemv', 's_dq_freeturnover']]
        self.indu = self.bandindu[['trade_dt', 's_info_windcode', 'induname1']]

        self.data_temp = pd.merge(self.mv, self.indu, how='left')
        self.data = pd.merge(self.data_temp, self.factor, how='left')

        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data_dum = pd.get_dummies(self.data)
        self.data_dum.drop('induname1_综合', axis=1, inplace=True)
        self.data_dum.reset_index(inplace=True)
        self.induname = [x for x in self.data_dum.columns if 'induname' in x]

        self.data_dum.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.data_merge = pd.merge(self.data_dum, self.mom20, how='left')
        self.data_merge = pd.merge(self.data_merge, self.std60, how='left')

        self.data_dropna = self.data_merge.dropna()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def neutral(self, data, y_item, x_item, name):
        temp = data.copy()
        y = temp[y_item]
        x = temp[x_item]
        x['intercept'] = 1
        model = sm.OLS(y, x).fit()
        return pd.DataFrame({'s_info_windcode': temp.s_info_windcode.values, name: model.resid})

    def compute(self):
        t = time.time()
        y_item = [self.fac_name]
        x_item = ['s_dq_freemv', 's_dq_freeturnover', 'mom20', 'std60'] + self.induname
        self.fac_name_n = self.fac_name + 'n'
        self.temp_result = self.data_dropna.groupby('trade_dt')\
                                           .apply(self.neutral, y_item, x_item, self.fac_name_n)\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', self.fac_name_n]
        self.result[item].to_pickle(self.save_indir + self.factor_name[:-4] + 'n.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

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
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'

    file_names = ['all_dayindex.pkl', 'all_band_indu.pkl']
    factor_names = ['factor_price_mom20.pkl', 'factor_price_std60.pkl']

    factor_name = 'factor_price_fr5.pkl'
    fn = FactorNeutral(file_indir, factor_indir, save_indir, file_names, factor_names, factor_name)
    fn.runflow()

    factor_name = 'factor_price_fr10.pkl'
    fn = FactorNeutral(file_indir, factor_indir, save_indir, file_names, factor_names, factor_name)
    fn.runflow()

    factor_name = 'factor_price_fr20.pkl'
    fn = FactorNeutral(file_indir, factor_indir, save_indir, file_names, factor_names, factor_name)
    fn.runflow()