import pandas as pd
import numpy as np
import time
import statsmodels.api as sm


# 因子中性化(市值+换手率+动量+波动率+行业）pre
class FactorNeutralPre(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_name, basic_factors):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_name = file_name
        self.basic_factors = basic_factors

    def filein(self):
        t = time.time()
        self.bandindu = pd.read_pickle(self.file_indir + self.file_name)
        self.fmv = pd.read_pickle(self.factor_indir + self.basic_factors[0])
        self.fto = pd.read_pickle(self.factor_indir + self.basic_factors[1])
        self.mom20 = pd.read_pickle(self.factor_indir + self.basic_factors[2])
        self.std20 = pd.read_pickle(self.factor_indir + self.basic_factors[3])
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.data_temp = pd.merge(self.fmv, self.fto, how='left')
        self.data_temp = pd.merge(self.data_temp, self.mom20, how='left')
        self.data_temp = pd.merge(self.data_temp, self.std20, how='left')
        item = ['trade_dt', 's_info_windcode', 'induname1']
        self.data = pd.merge(self.data_temp, self.bandindu[item], how='left')
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data_dum = pd.get_dummies(self.data)
        self.data_dum.drop('induname1_综合', axis=1, inplace=True)
        self.data_dum.reset_index(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.data_dum.to_pickle(self.save_indir + 'neutral_basic_data.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


# 因子中性化(市值+换手率+动量+波动率+行业）
class FactorNeutral(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_name, factor_name):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_name = file_name
        self.factor_name = factor_name

    def filein(self):
        t = time.time()
        self.basic = pd.read_pickle(self.file_indir + self.file_name)
        self.induname = [x for x in self.basic.columns if 'induname' in x]
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        self.fac_name = self.factor.columns[-1]
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.data_all = pd.merge(self.factor, self.basic, how='left')
        # 部分因子可能存在无穷结果，将其转换为空值
        self.data_all.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.data_dropna = self.data_all.dropna()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def method(self, data, y_item, x_item, name):
        temp = data.copy()
        y = temp[y_item]
        x = temp[x_item]
        x['intercept'] = 1
        model = sm.OLS(y, x).fit()
        return pd.DataFrame({'s_info_windcode': temp.s_info_windcode.values, name: model.resid})

    def compute(self):
        t = time.time()
        y_item = self.fac_name
        x_item = ['fmv', 'fto', 'mom20', 'std20'] + self.induname
        self.temp_result = self.data_dropna.groupby('trade_dt')\
                                           .apply(self.method, y_item, x_item, self.fac_name + 'n')\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.factor[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', self.fac_name + 'n']
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
    # 数据准备
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name = 'all_band_indu.pkl'
    basic_factors = ['factor_price_fmv.pkl', 'factor_price_fto.pkl',
                     'factor_price_mom20.pkl', 'factor_price_std20.pkl']

    fnp = FactorNeutralPre(file_indir, factor_indir, save_indir, file_name, basic_factors)
    fnp.runflow()

    # 中性化
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'
    file_name = 'neutral_basic_data.pkl'
    factor_name = 'factor_price_fr.pkl'

    fn = FactorNeutral(file_indir, factor_indir, save_indir, file_name, factor_name)
    fn.runflow()
