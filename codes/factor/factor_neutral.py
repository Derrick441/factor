import pandas as pd
import time
import statsmodels.api as sm
import os


# 因子中性化
class FactorNeutral(object):

    def __init__(self, indir_d, indir_f, indir_o, name_f):
        self.indir_d = indir_d
        self.indir_f = indir_f
        self.indir_o = indir_o
        self.name_f = name_f
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
        # 数据选取
        self.mv = self.dayindex[['trade_dt', 's_info_windcode', 's_dq_mv']]
        self.ind = self.inds[['trade_dt', 's_info_windcode', 'induname1']]
        # 取因子名
        self.factor_name = self.factor.columns.copy()[-1]
        # 数据重命名
        self.factor.columns = ['trade_dt', 's_info_windcode', 'factor']
        # 数据合并
        temp = pd.merge(self.factor, self.mv, how='left')
        self.data = pd.merge(temp, self.ind, how='left')
        # 数据独热处理
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data = pd.get_dummies(self.data)
        self.data.drop('induname1_综合', axis=1, inplace=True)
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
        mv_ind_item = list(set(self.data.columns)-set('factor'))
        item = self.factor_name + '_neutral'
        # 因子中性化处理
        self.data[item] = self.data.groupby(level=0).apply(self.neutral, 'factor', mv_ind_item).values
        self.data.reset_index(inplace=True)
        # 数据对齐
        temp1 = self.factor[['trade_dt', 's_info_windcode']]
        temp2 = self.data[['trade_dt', 's_info_windcode', item]]
        self.result = pd.merge(temp1, temp2, how='left')
        print('factor neutral running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result.to_pickle(self.indir_o + 'neutral_' + self.name_f)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.factorneutral()
        self.fileout()
        print('end running time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    # dataflow地址
    indir_dataflow = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    # factor地址和factor文件名
    indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    # factor中性化后输出地址
    indir_out = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'

    # 中性化一个因子
    name_factor = 'factor_price_bi.pkl'
    fn = FactorNeutral(indir_dataflow, indir_factor, indir_out, name_factor)
    fn.runflow()

    # # 中性化多个因子
    # name_factors = ['factor_price_Dkurt.pkl', 'factor_price_Dskew.pkl', 'factor_price_Dvol.pkl']
    # for i in name_factors:
    #     fn = FactorNeutral(indir_dataflow, indir_factor, indir_out, i)
    #     fn.runflow()

    # # 中性化全部因子
    # name_factors = os.listdir(indir_factor)
    # for i in name_factors:
    #     fn = FactorNeutral(indir_dataflow, indir_factor, indir_out, i)
    #     fn.runflow()
