import pandas as pd
import numpy as np
import time
import statsmodels.api as sm


# 抢跑因子FR：基于过去20日交易数据计算的T-1日换手率与T日涨跌幅的相关系数
class FactorFR0(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 股票日数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name[0])
        # 行业数据
        self.bandindu = pd.read_pickle(self.file_indir + self.file_name[1])
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.all_data.sort_values(['s_info_windcode', 'trade_dt'], inplace=True)
        # 昨日换手率
        self.all_data['s_dq_freeturnover_y'] = self.all_data.groupby('s_info_windcode')['s_dq_freeturnover'].shift(1)
        # 20日动量
        self.all_data['mom20'] = self.all_data.groupby('s_info_windcode')['s_dq_pctchange'].rolling(20).sum().values
        # 60日波动率
        self.all_data['std60'] = self.all_data.groupby('s_info_windcode')['s_dq_pctchange'].rolling(60).std().values
        # 加入行业变量
        item = ['trade_dt', 's_info_windcode', 'induname1']
        self.data_sum = pd.merge(self.all_data, self.bandindu[item], how='left')
        self.data_sum.set_index(['s_info_windcode', 'trade_dt'], inplace=True)
        self.data_dum = pd.get_dummies(self.data_sum)
        self.data_dum.drop('induname1_综合', axis=1, inplace=True)
        self.data_dum.reset_index(inplace=True)
        self.induname = [x for x in self.data_dum.columns if 'induname' in x]

        # 去除空值
        item = ['trade_dt', 's_info_windcode', 's_dq_pctchange', 's_dq_freeturnover_y',
                's_dq_freemv', 'mom20', 's_dq_freeturnover', 'std60'] + self.induname
        self.data_dropna = self.data_dum[item].dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def method(self, data, perid):
        temp = data.copy()
        result = temp[['s_dq_pctchange', 's_dq_freeturnover_y']].rolling(perid).corr().reset_index().iloc[::2, -1]
        return pd.DataFrame({'trade_dt': temp.trade_dt, 'fr': result.values})

    def compute(self):
        t = time.time()
        # 因子计算
        self.temp_result = self.data_dropna.groupby('s_info_windcode') \
                                           .apply(self.method, 20) \
                                           .reset_index()
        self.data_dropna1 = pd.merge(self.data_dropna, self.temp_result, how='left').dropna()
        print('compute running time:%10.4fs' % (time.time() - t))

    def method1(self, data, perid, index1, index2):
        temp = data.copy()
        num = len(data)
        if num > perid:
            temp['intercept'] = 1
            y = index1
            x = ['intercept'] + index2
            model = sm.OLS(temp[y], temp[x]).fit()
            return pd.DataFrame({'s_info_windcode': temp.s_info_windcode.values, 'fr0': model.resid})
        else:
            return pd.DataFrame({'s_info_windcode': temp.s_info_windcode.values, 'fr0': [None for i in range(num)]})

    def compute1(self):
        t = time.time()
        # 因子计算
        index1 = ['fr']
        index2 = ['s_dq_freemv', 'mom20', 's_dq_freeturnover', 'std60'] + self.induname
        self.temp_result1 = self.data_dropna1.groupby('trade_dt') \
                                             .apply(self.method1, 20, index1, index2) \
                                             .reset_index()
        print('compute1 running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result1, how='left')
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'fr0']
        self.result[item].to_pickle(self.save_indir + 'factor_price_fr0.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.compute1()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = ['all_dayindex.pkl', 'all_band_indu.pkl']

    fr0 = FactorFR0(file_indir, save_indir, file_name)
    fr0.runflow()
