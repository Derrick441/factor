import pandas as pd
import time
import statsmodels.api as sm
import os


# 因子中性化
class FactorNeutral(object):

    def __init__(self, indir_d, indir_f, name_f, indir_o):
        self.indir_d = indir_d
        self.indir_f = indir_f
        self.name_f = name_f
        self.indir_o = indir_o
        print(self.name_f)

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中读取市值、行业数据
        self.dayindex = pd.read_pickle(self.indir_d + 'all_dayindex.pkl')
        self.inds = pd.read_pickle(self.indir_d + 'all_band_indu.pkl')
        # 从factor文件夹中取因子数据
        self.factor = pd.read_pickle(self.indir_f + self.name_f)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.mv = self.dayindex[['trade_dt', 's_info_windcode', 's_dq_mv']]
        self.ind = self.inds[['trade_dt', 's_info_windcode', 'induname1']]
        self.factor_name = self.factor.columns.copy()[-1]
        self.factor.columns = ['trade_dt', 's_info_windcode', 'factor']
        self.data = pd.merge(self.factor, self.mv, how='left')
        self.data = pd.merge(self.data, self.ind, how='left')
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data = pd.get_dummies(self.data)
        self.data.dropna(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def neutral(self, data, y_item, x_item):
        y = data[y_item]
        x = data[x_item]
        x['intercept'] = 1
        result = sm.OLS(y, x).fit()
        return result.resid

    def factorneutral(self):
        t = time.time()
        # 取市值、行业列名
        mv_ind_item = list(set(self.data.columns)-set('factor'))
        # 因子中性化
        item = self.factor_name + '_neutral'
        self.data[item] = self.data.groupby(level=0).apply(self.neutral, 'factor', mv_ind_item).values
        self.data.reset_index(inplace=True)
        # 数据对齐
        temp1 = self.factor[['trade_dt', 's_info_windcode']]
        temp2 = self.data[['trade_dt', 's_info_windcode', item]]
        self.result = pd.merge(temp1, temp2, how='left')
        print('factor neutral running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 将因子中性化结果存储在factor文件夹中
        self.result.to_pickle(self.indir_o + 'neutral_' + self.name_f)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        self.filein()
        self.datamanage()
        self.factorneutral()
        self.fileout()


if __name__ == '__main__':
    # dataflow地址
    indir_dataflow = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    # factor地址和factor文件名
    indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    name_factor = 'factor_price_bi.pkl'
    # factor中性化后输出地址
    indir_out = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'

    fn = FactorNeutral(indir_dataflow, indir_factor, name_factor, indir_out)
    fn.runflow()

    # # 计算全部因子
    # name_factors = os.listdir(indir_factor)
    # for i in name_factors:
    #     fn = FactorNeutral(indir_dataflow, indir_factor, i, indir_out)
    #     fn.runflow()
