import pandas as pd
import numpy as np
import time
import statsmodels.api as sm
import os


# 因子中性化
class FactorNeutral(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_names, factor_name):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_names = file_names
        self.factor_name = factor_name
        print(self.factor_name)

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir + self.file_names[0])
        self.bandindu = pd.read_pickle(self.file_indir + self.file_names[1])
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.fac_name = self.factor.columns[-1]

        self.mv = self.all_data[['trade_dt', 's_info_windcode', 's_dq_freemv']]
        self.indu = self.bandindu[['trade_dt', 's_info_windcode', 'induname1']]

        self.data_temp = pd.merge(self.mv, self.indu, how='left')
        self.data = pd.merge(self.data_temp, self.factor, how='left')

        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data_dum = pd.get_dummies(self.data)
        self.data_dum.drop('induname1_综合', axis=1, inplace=True)
        self.data_dum.reset_index(inplace=True)
        self.induname = [x for x in self.data_dum.columns if 'induname' in x]

        self.data_dum.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.data_dum.dropna(inplace=True)
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
        x_item = ['s_dq_freemv'] + self.induname
        self.fac_name_n = 'neutral_' + self.fac_name
        self.temp_result = self.data_dum.groupby('trade_dt')\
                                        .apply(self.neutral, y_item, x_item, self.fac_name_n)\
                                        .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', self.fac_name_n]
        self.result[item].to_pickle(self.save_indir + 'neutral_' + self.factor_name)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


# 临时
# 计算因子值与收益值之间相关系数：IC
class PerfIc(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_name, factor_name, method, neutral):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_name = file_name
        self.factor_name = factor_name
        self.method = method
        self.neutral = neutral
        print(self.factor_name)
        print(self.file_name)

    def filein(self):
        t = time.time()
        self.ret = pd.read_pickle(self.file_indir + self.file_name)
        self.fac = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.ret_reset = self.ret.reset_index().rename(columns={0: 'ret'})
        self.data = pd.merge(self.ret_reset, self.fac, how='left')
        self.data.sort_values(by=['trade_dt', 's_info_windcode'], inplace=True)
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data.dropna(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def perf_single_ic(self, data, m):
        temp = data.copy()
        # IC
        if m == 'IC':
            result = temp.corr(method='pearson')
        # rank IC
        else:
            result = temp.corr(method='spearman')
        # 返回未来收益与因子的相关系数
        return result.values[0, 1]

    def compute(self):
        t = time.time()
        self.ic_name = self.fac.columns[-1] + '_' + self.method
        self.result = self.data.groupby(level=0)\
                               .apply(self.perf_single_ic, self.method)\
                               .reset_index()\
                               .rename(columns={0: self.ic_name})
        self.result.sort_values('trade_dt', inplace=True)
        self.result['accu'] = self.result[self.ic_name].cumsum()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 识别
        if self.method == 'IC':
            if self.neutral == 0:
                self.save = self.factor_name[:-4] + '_ic' + self.file_name[31:-4] + '.csv'
            else:
                self.save = self.factor_name[:-4] + '_ic' + self.file_name[4:-12] + '.csv'
        else:
            if self.neutral == 0:
                self.save = self.factor_name[:-4] + '_rankic' + self.file_name[31:-4] + '.csv'
            else:
                self.save = self.factor_name[:-4] + '_rankic' + self.file_name[4:-12] + '.csv'
        # 数据输出
        self.result.to_csv(self.save_indir + self.save)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time()-t))


class GroupTen(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_names, factor_name, perid):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_names = file_names
        self.factor_name = factor_name
        self.perid = perid
        print(self.factor_name)

    def filein(self):
        t = time.time()
        # 股票收益数据
        self.all_ret = pd.read_pickle(self.file_indir + self.file_names[0])
        # 行业数据
        self.all_indu = pd.read_pickle(self.file_indir + self.file_names[1])
        # 因子
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 名称调整
        self.fac_name = self.factor_name[:-4]
        self.factor.rename(columns={self.factor.columns[-1]: self.fac_name}, inplace=True)
        self.ret_name = 'ret' + str(self.perid)
        self.ret = self.all_ret.reset_index()
        self.ret.rename(columns={0: self.ret_name}, inplace=True)

        # 数据合并
        self.data = pd.merge(self.ret, self.all_indu, how='left')
        self.data = pd.merge(self.data, self.factor, how='left')
        # 去除空值
        item = ['trade_dt', 's_info_windcode', self.ret_name, 'induname1', self.fac_name]
        self.data_dropna = self.data[item].dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def groupten(self, data, factor, ret):
        temp = data.copy()
        temp['group'] = pd.cut(temp[factor].rank(), 10, labels=range(10))
        result = temp.groupby('group')[ret].mean()
        return result.values

    def compute(self):
        t = time.time()
        self.result = self.data_dropna.groupby(['trade_dt', 'induname1'])\
                                      .apply(self.groupten, self.fac_name, self.ret_name)\
                                      .apply(pd.Series)\
                                      .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        self.dayret = self.result.groupby('trade_dt').mean() / self.perid
        sign = 1 if (self.dayret[0].mean() > self.dayret[9].mean()) else -1
        self.dayret['dc'] = (self.dayret[0] - self.dayret[9])*sign
        self.dayret['accu0'] = self.dayret[0].cumsum()
        self.dayret['accu9'] = self.dayret[9].cumsum()
        self.dayret['accudc'] = self.dayret['dc'].cumsum()
        save = self.save_indir + 'group_ret_day\\' + self.fac_name + '_group10_ret_indu_' + str(self.perid) + '.csv'
        self.dayret.to_csv(save)

        self.mean_all = self.dayret.mean() * 250
        self.mean_1y = self.dayret[self.dayret.index >= '20190101'].mean() * 250
        self.mean_3y = self.dayret[self.dayret.index >= '20170101'].mean() * 250
        self.mean_5y = self.dayret[self.dayret.index >= '20150101'].mean() * 250

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'

    file_names = ['all_dayindex.pkl', 'all_band_indu.pkl']

    # # 中性化全部因子
    # temp_set1 = os.listdir(factor_indir)
    # factor_names = set([name for name in temp_set1 if 'pkl' in name])
    # for factor_name in factor_names:
    #     fn = FactorNeutral(file_indir, factor_indir, save_indir, file_names, factor_name)
    #     fn.runflow()

    # 中性化未中性化的因子
    temp_set1 = os.listdir(factor_indir)
    set1 = set([name for name in temp_set1 if 'pkl' in name])
    temp_set2 = os.listdir(save_indir)
    set2 = set([factorname[8:] for factorname in temp_set2])
    factor_names = set1 - set2

    for factor_name in factor_names:
        fn = FactorNeutral(file_indir, factor_indir, save_indir, file_names, factor_name)
        fn.runflow()

    # # 中性化一个因子
    # factor_names = ['factor_price_fr5.pkl']
    # for factor_name in factor_names:
    #     fn = FactorNeutral(file_indir, factor_indir, save_indir, file_names, factor_name)
    #     fn.runflow()

    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic_neutral\\'
    file_names = ['all_band_adjvwap_hh_price_label1.pkl',
                  'all_band_adjvwap_hh_price_label5.pkl',
                  'all_band_adjvwap_hh_price_label10.pkl',
                  'all_band_adjvwap_hh_price_label20.pkl',
                  'all_band_adjvwap_hh_price_label60.pkl']
    method = 'IC'
    neutral = 0

    # factor_names = os.listdir(factor_indir)
    # # 计算全部因子ic
    # for factor_name in factor_names:
    #     for file_name in file_names:
    #         ic = PerfIc(file_indir, factor_indir, save_indir, file_name, factor_name, method, neutral)
    #         ic.runflow()

    # 计算未计算ic因子的ic
    set1 = set(os.listdir(factor_indir))
    temp_set = os.listdir(save_indir)
    set2 = set([ic_name.split('_ic')[0] + '.pkl' for ic_name in temp_set])
    factor_names = set1 - set2

    for factor_name in factor_names:
        for file_name in file_names:
            ic = PerfIc(file_indir, factor_indir, save_indir, file_name, factor_name, method, neutral)
            ic.runflow()

    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\group_ret_neutral\\'

    perid_list = [1, 5, 10, 20, 60]
    file_names = ['all_band_adjvwap_hh_price_label1.pkl', 'all_band_adjvwap_hh_price_label5.pkl',
                  'all_band_adjvwap_hh_price_label10.pkl', 'all_band_adjvwap_hh_price_label20.pkl',
                  'all_band_adjvwap_hh_price_label60.pkl']
    temp = os.listdir(factor_indir)
    factor_names = [name for name in temp if 'pkl' in name]

    # 1日收益
    factor = []
    result_all = []
    result_1 = []
    result_3 = []
    result_5 = []
    for factor_name in factor_names:
        for i in range(5):
            perid = perid_list[i]
            file_names_i = [file_names[i], 'all_band_indu.pkl']
            g10 = GroupTen(file_indir, factor_indir, save_indir, file_names_i, factor_name, perid)
            g10.runflow()

            factor.append(g10.fac_name + 'ret' + str(perid))
            result_all.append(g10.mean_all)
            result_1.append(g10.mean_1y)
            result_3.append(g10.mean_3y)
            result_5.append(g10.mean_5y)
    # 格式整理
    out = pd.concat(result_all, axis=1).T
    out.rename(columns={'trade_dt': 'factor'}, inplace=True)
    out['factor'] = factor
    out.to_csv(save_indir + 'all_neutral_factors_group10_ret_mean_indu_allyears.csv')

    # 格式整理
    out_1 = pd.concat(result_1, axis=1).T
    out_1.rename(columns={'trade_dt': 'factor'}, inplace=True)
    out_1['factor'] = factor
    out_1.to_csv(save_indir + 'all_neutral_factors_group10_ret_mean_indu_1year.csv')

    # 格式整理
    out_3 = pd.concat(result_3, axis=1).T
    out_3.rename(columns={'trade_dt': 'factor'}, inplace=True)
    out_3['factor'] = factor
    out_3.to_csv(save_indir + 'all_neutral_factors_group10_ret_mean_indu_3years.csv')

    # 格式整理
    out_5 = pd.concat(result_5, axis=1).T
    out_5.rename(columns={'trade_dt': 'factor'}, inplace=True)
    out_5['factor'] = factor
    out_5.to_csv(save_indir + 'all_neutral_factors_group10_ret_mean_indu_5years.csv')
