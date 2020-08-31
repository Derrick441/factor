import pandas as pd
import time
import statsmodels.regression.rolling as regroll


# 日频率
# 特异度：1-收益率剔除市场因子、市值因子、估值因子回归模型的rsquared
class Ivr(object):

    def __init__(self, indir, index):
        self.indir = indir
        self.index = index

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中取股票日行情数据
        self.all_data = pd.read_pickle(self.indir + self.index + '/' + self.index + '_dayindex.pkl')
        # 从factor文件夹中取市场因子数据
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\mktfactor\\'
        self.all_mkt = pd.read_pickle(indir_factor + 'factor_mkt.pkl')
        self.all_smb = pd.read_pickle(indir_factor + 'factor_smb.pkl')
        self.all_hml = pd.read_pickle(indir_factor + 'factor_hml.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        # 数据合并
        temp_data = self.all_data[['trade_dt', 's_info_windcode', 'change']]
        self.data_sum = pd.merge(temp_data, self.all_mkt, how='left')
        self.data_sum = pd.merge(self.data_sum, self.all_smb, how='left')
        self.data_sum = pd.merge(self.data_sum, self.all_hml, how='left')
        print('data_manage running time:%10.4fs' % (time.time() - t))

    def rolling_regress(self, data):
        t = time.time()
        if len(data) >= 20:
            temp_data = data[data.change != 0]
            temp_data['intercept'] = 1
            item = ['intercept', 'mkt', 'smb', 'hml']
            model = regroll.RollingOLS(temp_data['change'], temp_data[item], window=20).fit()
            temp = 1 - model.rsquared
            result = pd.DataFrame({'trade_dt': temp_data.trade_dt.values, 'ivr': temp.values})
            print(time.time() - t)
            return result
        else:
            result = pd.DataFrame({'trade_dt': data.trade_dt.values, 'ivr': [None for i in range(len(data))]})
            return result

    def compute_ivr(self):
        t = time.time()
        # 每股滚动回归计算ivr
        self.result = self.data_sum.groupby('s_info_windcode').apply(self.rolling_regress)
        # 格式整理
        self.result.reset_index(inplace=True)
        self.result.drop('level_1', axis=1, inplace=True)
        print('compute_ivr running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.final = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.result, how='left')
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'ivr']
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        self.final[item].to_pickle(indir_factor + 'factor_price_ivr.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('compute start')
        self.filein()
        self.data_manage()
        self.compute_ivr()
        self.fileout()
        print('compute finish, all running time:%10.4fs' % (time.time() - t))
        return self


if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    index = 'all'
    ivr = Ivr(indir, index)
    ivr.runflow()
