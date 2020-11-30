import pandas as pd
import numpy as np
import time
import statsmodels.api as sm
import os


# 因子有效性检验：回归法
class TestRegression(object):

    def __init__(self, file_indir, factor_indir, save_indir, perid, ret_name, file_name, indu_name, factor_name):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.perid = perid
        self.ret_name = ret_name
        self.file_name = file_name
        self.indu_name = indu_name
        self.factor_name = factor_name
        print(self.factor_name)

    def filein(self):
        t = time.time()
        self.ret = pd.read_pickle(self.file_indir + self.ret_name)
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        self.bandindu = pd.read_pickle(self.file_indir + self.indu_name)
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.fac_name = self.factor.columns[-1]

        self.ret_name = 'ret_' + str(self.perid)
        self.ret_reset = self.ret.reset_index().rename(columns={0: self.ret_name})
        self.mv = self.all_data[['trade_dt', 's_info_windcode', 's_dq_freemv']]
        self.indu = self.bandindu[['trade_dt', 's_info_windcode', 'induname1']]

        self.data_temp = pd.merge(self.ret_reset, self.mv)
        self.data_temp = pd.merge(self.data_temp, self.indu, how='left')
        self.data = pd.merge(self.data_temp, self.factor, how='left')

        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data_dum = pd.get_dummies(self.data)
        self.data_dum.drop('induname1_综合', axis=1, inplace=True)
        self.data_dum.reset_index(inplace=True)
        self.induname = [x for x in self.data_dum.columns if 'induname' in x]

        self.data_dum.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.data_dum.dropna(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def compute_t(self, data, y_item, x_item):
        temp = data.copy()
        y = temp[y_item]
        x = temp[x_item]
        x['intercept'] = 1
        model = sm.OLS(y, x).fit(cov_type='HAC', cov_kwds={'maxlags': 3})
        return model.tvalues[-1]

    def compute(self):
        t = time.time()
        y_item = [self.ret_name]
        x_item = ['s_dq_freemv'] + self.induname + [self.fac_name]
        self.fac_name_t = self.fac_name + '_t'
        self.result = self.data_dum.groupby('trade_dt')\
                                   .apply(self.compute_t, y_item, x_item)\
                                   .reset_index()\
                                   .rename(columns={0: self.fac_name_t})
        self.t_mean = self.result[self.fac_name_t].mean()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result.to_csv(self.save_indir + self.fac_name_t + '_ret' + str(self.perid) + '.csv')
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
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\t_neutral\\'

    perid_list = [1, 5, 10, 20, 60]
    ret_names = ['all_band_adjvwap_hh_price_label1.pkl',
                 'all_band_adjvwap_hh_price_label5.pkl',
                 'all_band_adjvwap_hh_price_label10.pkl',
                 'all_band_adjvwap_hh_price_label20.pkl',
                 'all_band_adjvwap_hh_price_label60.pkl']
    file_name = 'all_dayindex.pkl'
    indu_name = 'all_band_indu.pkl'

    # 回归未回归的因子
    temp_set1 = os.listdir(factor_indir)
    set1 = set([name for name in temp_set1 if 'pkl' in name])
    temp_set2 = os.listdir(save_indir)
    set2 = set([factorname[:-6] + '.pkl' for factorname in temp_set2])
    factor_names = set1 - set2

    t_list = []
    t_name_list = []
    for factor_name in factor_names:
        for i in range(5):
            perid = perid_list[i]
            ret_name = ret_names[i]
            fn = TestRegression(file_indir, factor_indir, save_indir, perid, ret_name, file_name, indu_name, factor_name)
            fn.runflow()
            t_name_list.append(fn.fac_name + '_ret' + str(perid))
            t_list.append(fn.t_mean)
    result_sum = pd.DataFrame({'factor_ret': t_name_list, 't_mean': t_list})
    result_sum.to_csv(save_indir + 'all_factors_t_mean.pkl')
