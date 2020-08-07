import pandas as pd
import numpy as np
import time
import statsmodels.api as sm

class Pricedelay(object):

    def __init__(self, indir, INDEX, INDEX_mkt):
        self.indir = indir
        self.INDEX = INDEX
        self.INDEX_mkt = INDEX_mkt

    def fileIn(self):
        t = time.time()
        self.price = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_band_dates_stocks_closep.pkl')
        self.mkt_indexprice = pd.read_pickle(indir + self.INDEX_mkt + '/' + self.INDEX_mkt + '_indexprice.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        # 股票涨幅数据
        all_change_pivot = self.price.pivot('trade_dt', 's_info_windcode', 's_dq_close').pct_change() * 100
        all_change = all_change_pivot.stack().reset_index().rename(columns={0: 'stocks_rate'})
        # 市场涨幅数据
        mkt_rate = self.mkt_indexprice.reset_index()
        mkt_rate['index_rate'] = mkt_rate.s_dq_change / mkt_rate.s_dq_preclose * 100
        mkt_rate['index_rate1'] = mkt_rate['index_rate'].shift(1)
        mkt_rate['index_rate2'] = mkt_rate['index_rate'].shift(2)
        mkt_rate['index_rate3'] = mkt_rate['index_rate'].shift(3)
        # 汇总数据
        self.sum_data = pd.merge(all_change,
                                 mkt_rate[['trade_dt', 'index_rate', 'index_rate1', 'index_rate2', 'index_rate3']],
                                 how='left')
        print('data_manage running time:%10.4fs' % (time.time() - t))

    def regress(self, data, y, x):
        Y = data[y]
        X = data[x]
        X['intercept'] = 1
        result = sm.OLS(Y, X).fit()
        return result.rsquared

    def compute_pricedelay(self):
        t = time.time()
        # 提取年月标志
        self.sum_data['year_month'] = self.sum_data['trade_dt'].apply(lambda x: x[:6])
        # 去除na进行回归
        temp_sum_data = self.sum_data.dropna(how='any')
        # 按月度数据对每股进行回归，计算rsquared
        R_squared = temp_sum_data.groupby(['year_month', 's_info_windcode'])\
                                 .apply(self.regress, 'stocks_rate',['index_rate'])\
                                 .reset_index().rename(columns={0: 'R_1'})
        R_squared['R_3'] = temp_sum_data.groupby(['year_month', 's_info_windcode'])\
                                        .apply(self.regress, 'stocks_rate', ['index_rate', 'index_rate1',
                                                                             'index_rate2', 'index_rate3']).values
        # 根据rsquared，计算价格时滞
        R_squared['pricedelay'] = 1 - (R_squared.R_1 / R_squared.R_3)
        # 数据合并
        self.result = pd.merge(self.sum_data, R_squared, how='left')
        print('compute_pricedelay running time:%10.4fs' % (time.time() - t))

    def fileOut(self):
        t = time.time()
        self.result[['trade_dt','s_info_windcode','pricedelay']].to_pickle(self.indir + 'factor' + '/' + self.INDEX + '_pricedelay.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('compute start')
        self.fileIn()
        self.data_manage()
        self.compute_pricedelay()
        self.fileOut()
        print('compute finish, all running time:%10.4fs' % (time.time() - t))
        return self

if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    INDEX_mkt = 'zz500'
    pricedelay = Pricedelay(indir, INDEX, INDEX_mkt)
    pricedelay.runflow()