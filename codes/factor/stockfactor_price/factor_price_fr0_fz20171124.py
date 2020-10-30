import pandas as pd
import numpy as np
import time
import statsmodels.api as sm


# 因子中性化
class FactorFR0(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_names, factor_name):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_names = file_names
        self.factor_name = factor_name
        print(self.factor_name)

    def filein(self):
        t = time.time()
        # 读入市值、行业数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_names[0])
        self.bandindu = pd.read_pickle(self.file_indir + self.file_names[1])
        # 读入fr因子数据
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def mom20(self, data):
        temp = data.copy()
        result = 1
        for i in temp:
            result = result * (1+i/100)
        return (result-1)*100

    def datamanage(self):
        t = time.time()
        # 数据选取
        item = ['trade_dt', 's_info_windcode', 's_dq_freemv', 's_dq_pctchange', 's_dq_freeturnover']
        self.data_need = self.all_data[item]
        self.indu = self.bandindu[['trade_dt', 's_info_windcode', 'induname1']]
        # merge
        self.data_temp = pd.merge(self.data_need, self.indu, how='left')
        self.data = pd.merge(self.data_temp, self.factor, how='left')

        # 独热处理（行业独热，并去掉‘综合’行业）
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data_dum = pd.get_dummies(self.data)
        self.data_dum.drop('induname1_综合', axis=1, inplace=True)
        self.data_dum.reset_index(inplace=True)
        self.induname = [x for x in self.data_dum.columns if 'induname' in x]

        # 对数化自由流通市值
        self.data_dum['ln_freemv'] = np.log(self.data_dum['s_dq_freemv'])
        # 20日动量
        self.data_dum['mom20'] = self.data_dum.groupby('s_info_windcode')['s_dq_pctchange'] \
                                              .rolling(20) \
                                              .apply(self.mom20) \
                                              .values
        # 60日波动率
        self.data_dum['std60'] = self.data_dum.groupby('s_info_windcode')['s_dq_pctchange'] \
                                              .rolling(60) \
                                              .std() \
                                              .values

        # 去除无穷值、空值
        self.data_dum.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.data_dum.dropna(inplace=True)
        # 选取需要的数据
        item = ['trade_dt', 's_info_windcode', 'fr',
                'ln_freemv', 'mom20', 's_dq_freeturnover', 'std60'] + self.induname
        self.data_dropna = self.data_dum[item]
        print('datamanage running time:%10.4fs' % (time.time() - t))

    # 中性化回归函数
    def neutral(self, data, y_item, x_item, name):
        temp = data.copy()
        y = temp[y_item]
        x = temp[x_item]
        x['intercept'] = 1
        model = sm.OLS(y, x).fit()
        return pd.DataFrame({'s_info_windcode': temp.s_info_windcode.values, name: model.resid})

    def compute(self):
        t = time.time()
        # 中性化回归
        y_item = ['fr']
        x_item = ['ln_freemv', 'mom20', 's_dq_freeturnover', 'std60'] + self.induname
        self.temp_result = self.data_dropna.groupby('trade_dt')\
                                           .apply(self.neutral, y_item, x_item, 'fr0')\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', 'fr0']
        self.result[item].to_pickle(self.save_indir + 'factor_price_fr0.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

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
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'

    file_names = ['all_dayindex.pkl', 'all_band_indu.pkl']
    factor_name = 'factor_price_fr.pkl'

    fr0 = FactorFR0(file_indir, factor_indir, save_indir, file_names, factor_name)
    fr0.runflow()
