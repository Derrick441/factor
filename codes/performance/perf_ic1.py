import pandas as pd
import time
import statsmodels.api as sm


# 计算因子值与收益值之间相关系数（因子市值、行业中性化）
class PerformanceIc1(object):

    def __init__(self, indir_d, name_d, indir_f, name_f, indir_p, method):
        self.indir_d = indir_d
        self.name_d = name_d
        self.indir_f = indir_f
        self.name_f = name_f
        self.indir_p = indir_p
        self.method = method

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中读取收益、市值、行业数据
        self.ret = pd.read_pickle(self.indir_d + self.name_d[0])
        self.dayindex = pd.read_pickle(self.indir_d + self.name_d[1])
        self.inds = pd.read_pickle(self.indir_d + self.name_d[2])
        # 从factor文件夹中取因子数据
        self.factor = pd.read_pickle(self.indir_f + self.name_f)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        # 数据合并
        self.mv = self.dayindex[['trade_dt', 's_info_windcode', 's_dq_mv']]
        self.ind = self.inds[['trade_dt', 's_info_windcode', 'induname1']]
        self.factor_name = self.factor.columns[-1].copy()
        self.factor.columns = ['trade_dt', 's_info_windcode', 'factor']
        self.data = pd.merge(self.factor, self.ret, how='left')
        self.data = pd.merge(self.data, self.mv, how='left')
        self.data = pd.merge(self.data, self.ind, how='left')
        # 设index
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        # 行业独热处理
        self.data = pd.get_dummies(self.data)
        # 去除nan
        self.data.dropna(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def mv_ind_neutral(self, data, y_item, x_item):
        y = data[y_item]
        x = data[x_item]
        x['intercept'] = 1
        result = sm.OLS(y, x).fit()
        return result.resid

    def perf_single_ic(self, data, method):
        # IC
        if method == 'IC':
            ic = data.corr(method='pearson')
        # rank IC
        else:
            ic = data.corr(method='spearman')
        # 返回未来收益与因子的相关系数
        return ic.iloc[0, 1:]

    def compute_ic(self):
        t = time.time()
        # 因子市值、行业中性化
        f_ret_item = ['factor', 'ret_1', 'ret_5', 'ret_10', 'ret_20', 'ret_60']
        mv_ind_item = list(set(self.data.columns)-set(f_ret_item))
        self.data[self.factor_name] = self.data.groupby(level=0).apply(self.mv_ind_neutral, 'factor', mv_ind_item).values
        print('compute_ic(mv_indu_neutral) running time:%10.4fs' % (time.time() - t))

        # 计算每期因子ic或rank ic值
        item_ic = [self.factor_name, 'ret_1', 'ret_5', 'ret_10', 'ret_20', 'ret_60']
        self.result = self.data[item_ic].groupby(level=0).apply(self.perf_single_ic, method=self.method)
        print('compute_ic running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 将因子ic结果存储在performance文件夹中
        self.result.to_pickle(self.indir_p + self.method + '1_' + self.name_f)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        self.filein()
        self.datamanage()
        self.compute_ic()
        self.fileout()


# if __name__ == '__main__':
#     # dataflow数据地址：收益、市值、行业数据读取
#     indir_dataflow = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
#     name_dataflow = ['all_ret_sum.pkl', 'all_dayindex.pkl', 'all_indu.pkl']
#     # factor数据地址：因子数据读取
#     indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
#     name_factor = 'factor_price_ivff.pkl'
#     # performance数据地址：ic数据存放
#     indir_perf = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
#
#     performanceic1 = PerformanceIc1(indir_dataflow, name_dataflow, indir_factor, name_factor, indir_perf, 'IC')
#     performanceic1.runflow()
