import pandas as pd
import numpy as np
import time
import statsmodels.api as sm


# 准备重写###########################

# 月度的日频率
# 市值调整换手率：剔除换手率中市值影响
# 回归换手率和市值，取残差作为市值调整换手率
class TurnoverAdjusted(object):

    def __init__(self, file_indir, file_name):
        self.file_indir = file_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中取股票日行情数据
        self.price = pd.read_pickle(self.file_indir + self.file_name[0])
        self.all_band_price = pd.read_pickle(self.file_indir + self.file_name[1])
        self.all_fashre = pd.read_pickle(self.file_indir + self.file_name[2])
        self.all_famv = pd.read_pickle(self.file_indir + self.file_name[3])
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        # 取成交量数据
        data_volume = self.all_band_price[['trade_dt', 's_info_windcode', 's_dq_volume']]
        # 股本、市值数据调整
        data_fashre_reset = self.all_fashre.reset_index()
        data_famv_reset = self.all_famv.reset_index()
        # 数据合并
        self.data_sum = pd.merge(data_volume, data_fashre_reset, how='left')
        self.data_sum = pd.merge(self.data_sum, data_famv_reset, how='left')
        # 计算换手率
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
        t = time.time()
        # 提取月度标志
        self.temp_data_sum['year_month'] = self.temp_data_sum['trade_dt'].apply(lambda x: x[:6])
        # 按月度对每股（换手率-市值）进行回归，计算残差作为调整后的换手率
        temp = self.temp_data_sum.copy()
        temp.loc[:, 'turnoveradj'] = self.temp_data_sum.groupby(['year_month', 's_info_windcode'])\
                                                       .apply(self.regress, 'ln_turnover', ['ln_mv'])\
                                                       .values
        self.result_temp = pd.merge(self.data_sum, temp[['trade_dt', 's_info_windcode', 'turnoveradj']], how='left')
        print('compute_turnover_adjusted running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.price[['trade_dt', 's_info_windcode']], self.result_temp[item])
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'turnoveradj']
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        self.result[item].to_pickle(indir_factor + 'factor_price_turnoveradj.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.data_manage()
        self.compute_turnover_adjusted()
        self.fileout()
        print('end running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name = ['all_band_dates_stocks_closep.pkl', '_band_price.pkl', '_float_a_shr.pkl', '_float_a_mv.pkl']
    turnover_adjusted = TurnoverAdjusted(file_indir, file_name)
    turnover_adjusted.runflow()
