import pandas as pd
import time
import statsmodels.api as sm

# 月频率
# 特质波动率


class Ivff(object):

    def __init__(self, indir, index):
        self.indir = indir
        self.index = index

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.indir + self.index + '/' + self.index + '_dayindex.pkl')
        self.all_mkt = pd.read_pickle(self.indir + 'factor' + '/f4_' + 'zz500' + '_mkt.pkl')
        self.all_smb = pd.read_pickle(self.indir + 'factor' + '/f5_' + self.index + '_smb.pkl')
        self.all_hml = pd.read_pickle(self.indir + 'factor' + '/f6_' + self.index + '_hml.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        self.data_sum = pd.merge(self.all_data, self.all_mkt, how='left')
        self.data_sum = pd.merge(self.data_sum, self.all_smb, how='left')
        self.data_sum = pd.merge(self.data_sum, self.all_hml, how='left')
        print('data_manage running time:%10.4fs' % (time.time() - t))

    def regress(self, data, y, x):
        Y = data[y]
        X = data[x]
        X['intercept'] = 1
        result = sm.OLS(Y, X).fit()
        return result.resid.std()

    def rolling_regress(self, data):
        t = time.time()
        temp = data.rolling(20).apply(lambda x: self.regress(x, 'change', ['mkt', 'smb', 'hml']))
        print(time.time()-t)
        return temp

    def compute_ivff(self):
        t = time.time()
        self.result = self.data_sum.groupby('s_info_windcode').apply(self.rolling_regress)
        print('compute_ivff running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        pass
        # t = time.time()
        # self.result[['trade_dt', 's_info_windcode', 'ivff']].to_pickle(self.indir + 'factor' + '/f8_' + self.index + '_ivff.pkl')
        # print('fileout running time:%10.4fs' % (time.time()-t))

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

ivff.filein()
ivff.data_manage()
ivff.compute_ivff()
