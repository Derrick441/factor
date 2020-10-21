import pandas as pd
import numpy as np
import time
import statsmodels.api as sm
import os


# 因子中性化
class FactorNeutral(object):

    def __init__(self, file_indir1, file_name1s, file_indir2, file_name2, save_indir):
        self.file_indir1 = file_indir1
        self.file_name1s = file_name1s
        self.file_indir2 = file_indir2
        self.file_name2 = file_name2
        self.save_indir = save_indir
        print(self.file_name2)

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中读取:市值、行业和因子数据(每日数据)
        self.all_data = pd.read_pickle(self.file_indir1 + self.file_name1s[0])
        self.bandindu = pd.read_pickle(self.file_indir1 + self.file_name1s[1])
        self.factor = pd.read_pickle(self.file_indir2 + self.file_name2)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        # 因子名
        self.factor_name = self.factor.columns[-1]

        # 市值、行业数据选取
        self.mv = self.all_data[['trade_dt', 's_info_windcode', 's_dq_mv']]
        self.indu = self.bandindu[['trade_dt', 's_info_windcode', 'induname1']]

        # 以市值数据为基准，合并市值、行业和因子数据
        self.data_temp = pd.merge(self.mv, self.indu, how='left')
        self.data = pd.merge(self.data_temp, self.factor, how='left')
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)

        # 独热处理（行业独热，并去掉‘综合’行业）
        self.data_dum = pd.get_dummies(self.data)
        self.data_dum.drop('induname1_综合', axis=1, inplace=True)

        # 去除无穷值、空值
        self.data_dum.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.data_dum.dropna(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    # 中性化回归函数
    def neutral(self, data, y_item, x_item):
        temp = data.copy()
        y = temp[y_item]
        x = temp[x_item]
        x['intercept'] = 1
        result = sm.OLS(y, x).fit()
        return result.resid

    def compute(self):
        t = time.time()
        # 中性化回归
        y_item = [self.factor_name]
        x_item = list(set(self.data_dum.columns)-set(y_item))
        self.temp_result = self.data_dum.groupby(level=0)\
                                        .apply(self.neutral, y_item, x_item)\
                                        .droplevel(0)\
                                        .reset_index()\
                                        .rename(columns={0: 'neutral_' + self.factor_name})
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', 'neutral_' + self.factor_name]
        self.result[item].to_pickle(self.save_indir + 'neutral_' + self.file_name2)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir1 = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_indir2 = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'

    file_name1s = ['all_dayindex.pkl', 'all_band_indu.pkl']

    # # 中性化全部因子
    # file_name2s = os.listdir(file_indir2)

    # 中性化未中性化的因子
    set1 = set(os.listdir('D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'))
    temp = set(os.listdir('D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'))
    set2 = set([])
    for filename in temp:
        set2.add(filename[8:])
    file_name2s = set1 - set2

    for file_name2 in file_name2s:
        fn = FactorNeutral(file_indir1, file_name1s, file_indir2, file_name2, save_indir)
        fn.runflow()