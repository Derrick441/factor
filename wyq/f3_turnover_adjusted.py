import pandas as pd
import numpy as np
import time
import statsmodels.api as sm

# 市值调整换手率：剔除换手率中市值影响
# 回归换手率和市值，取残差作为市值调整换手率
class Turnover_adjusted(object):

    def __init__(self, indir, INDEX):
        self.indir = indir
        self.INDEX = INDEX

    def filein(self):
        t = time.time()
        self.all_band_price = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_band_price.pkl')
        self.all_fashre = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_float_a_shr.pkl')
        self.all_famv = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_float_a_mv.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        # 取A股成交量数据
        data_volume = self.all_band_price[['trade_dt', 's_info_windcode', 's_dq_volume']]
        # reset_index()
        data_fashre_reset = self.all_fashre.reset_index()
        data_famv_reset = self.all_famv.reset_index()
        # 数据合并
        self.data_sum = pd.merge(data_volume, data_fashre_reset, how='left')
        self.data_sum = pd.merge(self.data_sum, data_famv_reset, how='left')
        # 换手率计算
        self.data_sum['turnover'] = self.data_sum.s_dq_volume / self.data_sum.float_a_shr
        # 去除nan和0
        self.temp_data_sum = self.data_sum.dropna(how='any')
        self.temp_data_sum = self.temp_data_sum[self.temp_data_sum.turnover != 0]
        self.temp_data_sum = self.temp_data_sum[self.temp_data_sum.float_a_mv != 0]
        # 对数化
        self.temp_data_sum['ln_turnover'] = np.log(self.temp_data_sum.turnover)
        self.temp_data_sum['ln_mv'] = np.log(self.temp_data_sum.float_a_mv)
        print('data_manage running time:%10.4fs' % (time.time() - t))

    def regress(self, data, y, x):
        Y = data[y]
        X = data[x]
        X['intercept'] = 1
        result = sm.OLS(Y, X).fit()
        return result.resid

    def compute_turnover_adjusted(self):
        t0 = time.time()
        t = time.time()
        # 提取年月标志
        self.temp_data_sum['year_month'] = self.temp_data_sum['trade_dt'].apply(lambda x: x[:6])
        print(time.time() - t)

        t = time.time()
        # 按月度数据对每股进行回归，计算回归残差，并将其作为调整后的换手率
        temp = self.temp_data_sum.copy()
        temp.loc[:, 'turnover_adjusted'] = self.temp_data_sum.groupby(['year_month', 's_info_windcode'])\
                                                             .apply(self.regress, 'ln_turnover', ['ln_mv'])\
                                                             .values
        print(time.time()-t)

        t = time.time()
        self.result = pd.merge(self.data_sum, temp[['trade_dt', 's_info_windcode', 'turnover_adjusted']], how='left')
        print(time.time() - t)

        print('compute_turnover_adjusted running time:%10.4fs' % (time.time() - t0))

    def fileout(self):
        t = time.time()
        self.result[['trade_dt','s_info_windcode','turnover_adjusted']].to_pickle(self.indir + 'factor' + '/f3_' + self.INDEX + '_turnover_adjusted.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('compute start')
        self.filein()
        self.data_manage()
        self.compute_turnover_adjusted()
        self.fileout()
        print('compute finish, all running time:%10.4fs' % (time.time() - t))
        return self

if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    turnover_adjusted = Turnover_adjusted(indir, INDEX)
    turnover_adjusted.runflow()

