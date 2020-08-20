import pandas as pd
import numpy as np
import time
import statsmodels.regression.rolling as regroll

# 日频率
# 特质波动率


class Ivff(object):

    def __init__(self, indir, index):
        self.indir = indir
        self.index = index

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.indir + self.index + '/' + self.index + '_dayindex.pkl')
        # 从factor文件夹中取因子数据
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\basicfactor\\'
        self.all_mkt = pd.read_pickle(indir_factor + 'factor_mkt.pkl')
        self.all_smb = pd.read_pickle(indir_factor + 'factor_smb.pkl')
        self.all_hml = pd.read_pickle(indir_factor + 'factor_hml.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        self.data_sum = pd.merge(self.all_data, self.all_mkt, how='left')
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
            temp = np.sqrt(model.mse_resid)*np.sqrt(243)
            result = pd.DataFrame({'trade_dt': temp_data.trade_dt.values, 'ivff': temp.values})
            print(time.time() - t)
            return result
        else:
            result = pd.DataFrame({'trade_dt': data.trade_dt.values, 'ivff': [None for i in range(len(data))]})
            return result

    def compute_ivff(self):
        t = time.time()
        self.result = self.data_sum.groupby('s_info_windcode').apply(self.rolling_regress)
        # 格式整理
        self.result.reset_index(inplace=True)
        self.result.drop('level_1', axis=1, inplace=True)
        print('compute_ivff running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 存在factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'ivff']
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        self.result[item].to_pickle(indir_factor + 'factor_price_ivff.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('compute start')
        self.filein()
        self.data_manage()
        self.compute_ivff()
        self.fileout()
        print('compute finish, all running time:%10.4fs' % (time.time() - t))
        return self


if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    index = 'all'
    ivff = Ivff(indir, index)
    ivff.runflow()
