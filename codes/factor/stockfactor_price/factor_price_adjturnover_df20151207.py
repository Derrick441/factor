import pandas as pd
import numpy as np
import time
import statsmodels.regression.rolling as regroll


# 调整换手率：剔除换手率中市值影响
class AdjTurnover(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 股票日数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 0值转空值
        item = ['s_dq_freeturnover', 's_dq_freemv']
        self.all_data[item] = self.all_data[item].replace(0, np.nan)
        # 对数化
        self.all_data['ln_turnover'] = np.log(self.all_data['s_dq_freeturnover'])
        self.all_data['ln_freemv'] = np.log(self.all_data['s_dq_freemv'])
        # 数据选取
        self.data = self.all_data[['trade_dt', 's_info_windcode', 'ln_turnover', 'ln_freemv']].copy()
        # 去除nan
        self.data_dropna = self.data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def roll_regress(self, data, perid):
        t = time.time()
        temp = data.copy()
        num = len(temp)
        if num > perid:
            temp['intercept'] = 1
            item = ['intercept', 'ln_freemv']
            # 滚动回归
            model = regroll.RollingOLS(temp['ln_turnover'], temp[item], window=perid).fit()
            # 根据回归参数计算调整换手率
            coef = model.params
            temp['adjturnover'] = temp['ln_turnover']-(coef.intercept*1+coef.ln_freemv*temp['ln_freemv'])
            print(time.time() - t)
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, 'adjturnover': temp.adjturnover.values})
        else:
            return pd.DataFrame({'trade_dt': temp.trade_dt.values, 'adjturnover': [None for i in range(num)]})

    def compute(self):
        t = time.time()
        # 滚动回归计算残差作为调整换手率
        self.temp_result = self.data_dropna.groupby('s_info_windcode')\
                                           .apply(self.roll_regress, 20)\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', 'adjturnover']
        self.result[item].to_pickle(self.save_indir + 'factor_price_adjturnover.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_dayindex.pkl'

    at = AdjTurnover(file_indir, save_indir, file_name)
    at.runflow()
