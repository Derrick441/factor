import pandas as pd
import numpy as np
import time
import statsmodels.regression.rolling as regroll


# 日频率
# 市值调整换手率：剔除换手率中市值影响
# 回归换手率和市值，取残差作为市值调整换手率
class AdjTurnover(object):

    def __init__(self, file_indir, file_name):
        self.file_indir = file_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中取股票日行情数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        # 0值处理
        item = ['s_dq_freeturnover', 's_dq_mv']
        self.all_data[item] = self.all_data[item].replace(0, np.nan)
        # 对数化
        self.all_data['ln_turnover'] = np.log(self.all_data.s_dq_freeturnover)
        self.all_data['ln_mv'] = np.log(self.all_data.s_dq_mv)
        print('data_manage running time:%10.4fs' % (time.time() - t))

    def rolling_regress(self, data):
        t = time.time()
        temp_data = data[['ln_turnover', 'ln_mv']].dropna().copy()
        if len(temp_data) >= 20:
            temp_data['intercept'] = np.ones(len(temp_data))
            item = ['intercept', 'ln_mv']
            # 滚动回归
            model = regroll.RollingOLS(temp_data['ln_turnover'], temp_data[item], window=20).fit()
            coef = model.params
            # 根据回归参数计算残差
            temp_data['adjturnover'] = temp_data.ln_turnover-(coef.intercept+coef.ln_mv*temp_data.ln_mv)
            # 数据对齐
            result = pd.merge(data, temp_data, how='left')
            print(time.time() - t)
            return result[['trade_dt', 'adjturnover']]
        else:
            result = pd.DataFrame({'trade_dt': data.trade_dt.values, 'adjturnover': [None for i in range(len(data))]})
            return result

    def compute_turnover_adjusted(self):
        t = time.time()
        # 滚动回归计算残差作为调整换手率
        self.result_temp = self.all_data.groupby('s_info_windcode').apply(self.rolling_regress)
        # 格式整理
        self.result_temp.reset_index(inplace=True)
        self.result_temp.drop('level_1', axis=1, inplace=True)
        print('compute_turnover_adjusted running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.result_temp, how='left')
        # 输出到factor文件夹的stockfactor中
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        item = ['trade_dt', 's_info_windcode', 'adjturnover']
        self.result[item].to_pickle(indir_factor + 'factor_price_adjturnover.pkl')
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
    file_name = 'all_dayindex.pkl'
    adjturnover = AdjTurnover(file_indir, file_name)
    adjturnover.runflow()
