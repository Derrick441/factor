import pandas as pd
import time
import statsmodels.api as sm

# 月频率
# 价格时滞：股价对市场信息的反应存在时滞，计算过去市场收益率对股票收益率的解释程度
# 取一个月收益率数据，回归t0股票收益率与t0市场收益率，得到r_squared解释系数R1
# 取一个月收益率数据，回归t0股票收益率与t0、t-1、t-2和t-3市场收益率，得到r_squared解释系数R3
# 1-(R1/R3)


class PriceDelay(object):

    def __init__(self, indir, index, index_mkt):
        self.indir = indir
        self.index = index
        self.index_mkt = index_mkt

    def filein(self):
        t = time.time()
        self.price = pd.read_pickle(self.indir + self.index + '/' + self.index + '_band_dates_stocks_closep.pkl')
        self.mkt = pd.read_pickle(self.indir + 'factor' + '/f4_' + 'zz500' + '_mkt.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        # 股票涨幅数据
        all_change_pivot = self.price.pivot('trade_dt', 's_info_windcode', 's_dq_close').pct_change() * 100
        all_change = all_change_pivot.stack().reset_index().rename(columns={0: 'stocks_rate'})
        # 市场涨幅数据
        mkt_rate = self.mkt.copy()
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
        temp_sum_data = self.sum_data.copy().dropna()
        # 按月度数据对每股进行回归，计算rsquared
        r_squared = temp_sum_data.groupby(['year_month', 's_info_windcode'])\
                                 .apply(self.regress, 'stocks_rate', ['index_rate'])\
                                 .reset_index().rename(columns={0: 'R_1'})
        r_squared['R_3'] = temp_sum_data.groupby(['year_month', 's_info_windcode'])\
                                        .apply(self.regress, 'stocks_rate', ['index_rate', 'index_rate1',
                                                                             'index_rate2', 'index_rate3'])\
                                        .values
        # 根据rsquared，计算价格时滞
        r_squared['pricedelay'] = 1 - (r_squared.R_1 / r_squared.R_3)
        # 数据合并
        self.result = pd.merge(self.sum_data, r_squared, how='left')
        print('compute_pricedelay running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        item = ['trade_dt', 's_info_windcode', 'pricedelay']
        self.result[item].to_pickle(self.indir + 'factor' + '/f2_' + self.index + '_pricedelay.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('compute start')
        self.filein()
        self.data_manage()
        self.compute_pricedelay()
        self.fileout()
        print('compute finish, all running time:%10.4fs' % (time.time() - t))
        return self


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    file_index = 'all'
    file_index_mkt = 'zz500'
    pricedelay = PriceDelay(file_indir, file_index, file_index_mkt)
    pricedelay.runflow()
