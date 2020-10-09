import pandas as pd
import numpy as np
import time


# 5日均价偏差apb: 5日的成交量加权平均价的平均价/5日的成交量加权的成交量加权平均价的平均价
class ApbNd(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 股票数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        item = ['trade_dt', 's_info_windcode', 's_dq_amount', 's_dq_volume']
        # 0->nan
        self.data = self.all_data[item].replace(0, np.nan).copy()
        # 去除nan
        self.data_dropna = self.data.dropna().copy()
        # 日成交量加权平均价(千元/百股*10=元/股）
        self.data_dropna['vwap'] = self.data_dropna['s_dq_amount'] / self.data_dropna['s_dq_volume'] * 10
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def roll_apb(self, data, index, weight, perid):
        temp = data.copy()
        num = len(temp)
        if num > perid:
            # 计算perid日的日成交量加权平均价的平均价
            temp['mean'] = temp[index].rolling(perid).mean()

            # 计算perid日的成交量加权的日成交量加权平均价的平均价
            temp['sum'] = temp[weight].rolling(perid).sum()
            temp['temp'] = (temp[weight] / temp['sum']) * temp[index]
            temp['weight_mean'] = temp['temp'].rolling(perid).sum()

            # 0->nan
            temp['mean'] = temp['mean'].replace(0, np.nan)
            temp['weight_mean'] = temp['weight_mean'].replace(0, np.nan)

            # 计算apb
            name = 'apb' + str(perid) + 'd'
            temp[name] = np.log(temp['mean'] / temp['weight_mean'])

            result = temp[['trade_dt', name]].copy()
            return result
        else:
            name = 'apb' + str(perid) + 'd'
            result = pd.DataFrame({'trade_dt': temp.trade_dt.values, name: [None for i in range(num)]})
            return result

    def compute(self):
        t = time.time()
        self.temp_result_5 = self.data_dropna.groupby('s_info_windcode')\
                                             .apply(self.roll_apb, 'vwap', 's_dq_volume', 5)\
                                             .reset_index()
        self.temp_result_20 = self.data_dropna.groupby('s_info_windcode')\
                                              .apply(self.roll_apb, 'vwap', 's_dq_volume', 20)\
                                              .reset_index()
        print('compute using time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.all_data = pd.read_pickle(self.file_indir + 'all_dayindex.pkl')
        self.result_5 = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result_5, how='left')
        self.result_20 = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result_20, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', 'apb5d']
        self.result_5[item].to_pickle(self.save_indir + 'factor_price_apb5d.pkl')
        item = ['trade_dt', 's_info_windcode', 'apb20d']
        self.result_20[item].to_pickle(self.save_indir + 'factor_price_apb20d.pkl')
        print('fileout using time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_band_price.pkl'

    apb = ApbNd(file_indir, save_indir, file_name)
    apb.runflow()

# apb.filein()
# apb.datamanage()
# apb.compute()
