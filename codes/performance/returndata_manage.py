import pandas as pd
import time
import statsmodels.api as sm


# 汇总5个收益率数据，并对其进行中性化处理
class RetMergeNeutral(object):

    def __init__(self, indir_d):
        self.indir_d = indir_d

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中读取return数据
        self.ret_1 = pd.read_pickle(self.indir_d + 'all_band_price_label1.pkl')
        self.ret_5 = pd.read_pickle(self.indir_d + 'all_band_price_label5.pkl')
        self.ret_10 = pd.read_pickle(self.indir_d + 'all_band_price_label10.pkl')
        self.ret_20 = pd.read_pickle(self.indir_d + 'all_band_price_label20.pkl')
        self.ret_60 = pd.read_pickle(self.indir_d + 'all_band_price_label60.pkl')
        # 从dataflow文件夹中读取市值、行业数据
        self.dayindex = pd.read_pickle(self.indir_d + 'all_dayindex.pkl')
        self.inds = pd.read_pickle(self.indir_d + 'all_band_indu.pkl')
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamange_merge(self):
        # 收益数据合并
        t = time.time()
        temp1 = self.ret_1.reset_index().rename(columns={0: 'ret_1'})
        temp5 = self.ret_5.reset_index().rename(columns={0: 'ret_5'})
        temp10 = self.ret_10.reset_index().rename(columns={0: 'ret_10'})
        temp20 = self.ret_20.reset_index().rename(columns={0: 'ret_20'})
        temp60 = self.ret_60.reset_index().rename(columns={0: 'ret_60'})
        data = pd.merge(temp1, temp5, how='left')
        data = pd.merge(data, temp10, how='left')
        data = pd.merge(data, temp20, how='left')
        data = pd.merge(data, temp60, how='left')
        self.result1 = data
        print('datamanage-comebine running time:%10.4fs' % (time.time() - t))

    def neutral(self, data, y_item, x_item):
        y = data[y_item]
        x = data[x_item]
        x['intercept'] = 1
        result = sm.OLS(y, x).fit()
        return result.resid

    def datamanage_neutral(self):
        # 数据处理
        t = time.time()
        self.mv = self.dayindex[['trade_dt', 's_info_windcode', 's_dq_mv']]
        self.ind = self.inds[['trade_dt', 's_info_windcode', 'induname1']]
        self.data = pd.merge(self.result1, self.mv, how='left')
        self.data = pd.merge(self.data, self.ind, how='left')
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data = pd.get_dummies(self.data)
        self.data.drop('induname1_综合', axis=1, inplace=True)
        self.data.dropna(inplace=True)
        # 中性化回归
        ret_item = ['ret_1', 'ret_5', 'ret_10', 'ret_20', 'ret_60']
        mv_ind_item = list(set(self.data.columns)-set(ret_item))
        for i in ret_item:
            print(i)
            self.data[i] = self.data.groupby(level=0).apply(self.neutral, i, mv_ind_item).values
        self.result2 = self.data[ret_item]
        self.result2.reset_index(inplace=True)
        print('datamanage-neutral running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result1.to_pickle(self.indir_d + 'all_ret_sum.pkl')
        self.result2.to_pickle(self.indir_d + 'all_ret_sum_neutral.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        self.filein()
        self.datamange_merge()
        self.datamanage_neutral()
        self.fileout()


if __name__ == '__main__':
    indir_dataflow = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    ret_manage = RetMergeNeutral(indir_dataflow)
    ret_manage.runflow()
