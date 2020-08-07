import pandas as pd
import numpy as np
import time
import statsmodels.api as sm

class Turnover_adjusted(object):

    def __init__(self, indir, INDEX):
        self.indir = indir
        self.INDEX = INDEX

    def fileIn(self):
        t = time.time()

        self.all_turnover = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_XXX.pkl')
        self.all_price = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_band_dates_stocks_closep.pkl')
        self.all_share = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_XXX.pkl')

        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        print('data_manage running time:%10.4fs' % (time.time() - t))
        # 格式处理

        # 市值计算

        # 对数化，合并
        self.data_sum=1


    def regress(self, data, y, x):
        Y = data[y]
        X = data[x]
        X['intercept'] = 1
        result = sm.OLS(Y, X).fit()
        return result.resid

    def compute_turnover_adjusted(self):
        t = time.time()
        # 提取年月标志
        self.data_sum['year_month'] = self.data_sum['trade_dt'].apply(lambda x: x[:6])
        # 去除na进行回归
        temp_data_sum = self.data_sum.dropna(how='any')
        # 按月度数据对每股进行回归，计算rsquared
        temp = temp_data_sum.groupby(['year_month', 's_info_windcode'])\
                             .apply(self.regress, 'ln_turnover',['ln_mv'])\
                             .reset_index().rename(columns={0: 'turnover_adjusted'})
        # ???

        self.result = pd.merge(self.data_sum, temp, how='left')
        print('compute_pricedelay running time:%10.4fs' % (time.time() - t))

    def fileOut(self):
        t = time.time()
        self.result[['trade_dt','s_info_windcode','turnover_adjusted']].to_pickle(self.indir + 'factor' + '/' + self.INDEX + '_turnover_adjusted.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('compute start')
        self.fileIn()
        self.data_manage()
        self.compute_turnover_adjusted()
        self.fileOut()
        print('compute finish, all running time:%10.4fs' % (time.time() - t))
        return self

if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    turnover_adjusted = Turnover_adjusted(indir, INDEX)
    turnover_adjusted.runflow()