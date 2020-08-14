import pandas as pd
import numpy as np
import time
import statsmodels.api as sm

# 月频率

# 特质波动率
class Ivff(object):

    def __init__(self, indir, INDEX):
        self.indir = indir
        self.INDEX = INDEX

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_day_mv_pe_pb_turn_close.pkl')
        self.all_mkt = pd.read_pickle(self.indir + 'factor' + '/f4_' + 'zz500' + '_mkt.pkl')
        self.all_smb = pd.read_pickle(self.indir + 'factor' + '/f5_' + self.INDEX + '_smb.pkl')
        self.all_hml = pd.read_pickle(self.indir + 'factor' + '/f6_' + self.INDEX + '_hml.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        self.all_data_price = self.all_data.s_dq_close_today.to_frame().reset_index()
        self.all_data_change_pivot = self.all_data_price.pivot('trade_dt','s_info_windcode','s_dq_close_today').pct_change()
        self.all_data_change = self.all_data_change_pivot.stack().reset_index().rename(columns={0:'change'})

        self.data_sum = pd.merge(self.all_data_change, self.all_mkt, how='left')
        self.data_sum = pd.merge(self.data_sum, self.all_smb, how='left')
        self.data_sum = pd.merge(self.data_sum, self.all_hml, how='left')
        print('data_manage running time:%10.4fs' % (time.time() - t))

    def regress(self, data, y, x):
        Y = data[y]
        X = data[x]
        X['intercept'] = 1
        result = sm.OLS(Y, X).fit()
        return result.resid.std()

    def compute_ivff(self):
        t0 = time.time()
        t = time.time()
        # 提取年月标志
        self.data_sum['year_month'] = self.data_sum['trade_dt'].apply(lambda x: x[:6])
        print(time.time() - t)

        t = time.time()
        # 按月度数据对每股进行回归，计算特质波动率
        temp = self.data_sum.copy().dropna()
        ivff = temp.groupby(['year_month', 's_info_windcode'])\
                   .apply(self.regress, 'change', ['index_rate', 'smb', 'hml'])\
                   .reset_index().rename(columns={0: 'ivff'})
        print(time.time()-t)

        t = time.time()
        self.result = pd.merge(self.data_sum, ivff, how='left')
        self.result.ivff = self.result.ivff * pow(243, 0.5)
        print(time.time() - t)

        print('compute_turnover_adjusted running time:%10.4fs' % (time.time() - t0))

    def fileout(self):
        t = time.time()
        self.result[['trade_dt', 's_info_windcode', 'ivff']].to_pickle(self.indir + 'factor' + '/f8_' + self.INDEX + '_ivff.pkl')
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
    INDEX = 'all'
    ivff = Ivff(indir, INDEX)
    ivff.runflow()
