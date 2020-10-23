import pandas as pd
import time
import statsmodels.api as sm


# 收益率中性化
class ReturnNeutral(object):

    def __init__(self, file_indir, file_names1, file_name2):
        self.file_indir = file_indir
        self.file_names1 = file_names1
        self.file_name2 = file_name2
        print(self.file_name2)

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中读取:市值、行业和收益率数据(每日数据)
        self.all_data = pd.read_pickle(self.file_indir + self.file_names1[0])
        self.bandindu = pd.read_pickle(self.file_indir + self.file_names1[1])
        self.ret = pd.read_pickle(self.file_indir + self.file_name2)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamange(self):
        t = time.time()
        # 收益名
        temp = self.file_name2[31:]
        self.ret_name = 'ret_' + temp[:-4]
        self.ret_reset = self.ret.reset_index().rename(columns={0: self.ret_name})

        # 数据选取
        self.mv = self.all_data[['trade_dt', 's_info_windcode', 's_dq_mv']]
        self.indu = self.bandindu[['trade_dt', 's_info_windcode', 'induname1']]

        # 以市值数据的长度为基准，合并市值、行业和收益率数据
        self.data_temp = pd.merge(self.mv, self.indu, how='left')
        self.data = pd.merge(self.data_temp, self.ret_reset, how='left')
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)

        # 独热处理（行业独热，并去掉‘综合’行业）
        self.data_dum = pd.get_dummies(self.data)
        self.data_dum.drop('induname1_综合', axis=1, inplace=True)

        # 去除空值
        self.data_dum.dropna(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    # 中性化回归函数
    def neutral(self, data, y_item, x_item):
        y = data[y_item]
        x = data[x_item]
        x['intercept'] = 1
        result = sm.OLS(y, x).fit()
        return result.resid

    def compute(self):
        t = time.time()
        # 中性化回归
        y_item = [self.ret_name]
        x_item = list(set(self.data_dum.columns)-set(y_item))
        self.temp_result = self.data_dum.groupby(level=0)\
                                        .apply(self.neutral, y_item, x_item)\
                                        .droplevel(0)\
                                        .reset_index()\
                                        .rename(columns={0: self.ret_name + '_neutral'})
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', self.ret_name + '_neutral']

        # 使中性化收益数据和收益数据的格式保持一致
        self.output = self.result[item].copy()
        self.output.set_index(['trade_dt', 's_info_windcode'], inplace=True)

        self.output.to_pickle(self.file_indir + self.ret_name + '_neutral.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.datamange()
        self.compute()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_names1 = ['all_dayindex.pkl', 'all_band_indu.pkl']
    file_names2 = ['all_band_adjvwap_hh_price_label1.pkl',
                   'all_band_adjvwap_hh_price_label5.pkl',
                   'all_band_adjvwap_hh_price_label10.pkl',
                   'all_band_adjvwap_hh_price_label20.pkl',
                   'all_band_adjvwap_hh_price_label60.pkl']

    for i in file_names2:
        rn = ReturnNeutral(file_indir, file_names1, i)
        rn.runflow()
