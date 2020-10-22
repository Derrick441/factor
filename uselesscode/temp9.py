import pandas as pd
import numpy as np
import time


class TradeWeightPre(object):

    def __init__(self, file_indir, zz500_indir, factor_indir, save_indir, file_name, zz500_name, set):
        self.file_indir = file_indir
        self.zz500_indir = zz500_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_name = file_name
        self.zz500_name = zz500_name
        self.factor_name = set[0]
        self.num = set[1]
        self.weight_name = set[2]

    def filein(self):
        t = time.time()
        # 股票日数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name[0])
        # 股票行业数据
        self.all_indu = pd.read_pickle(self.file_indir + self.file_name[1])
        # zz500权重数据
        self.zz500_weight = pd.read_pickle(self.zz500_indir + self.zz500_name)
        # 因子值
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def weight_compute(self, data, index, num):
        temp = data.copy()
        temp['group'] = pd.cut(temp[index].rank(), 10, labels=range(10))
        temp['flag'] = temp['group'].apply(lambda x: 1 if x >= num else 0)
        weight_hold_sum = np.sum(temp[temp['group'] >= num]['weightedratio'])
        weight_all_sum = np.sum(temp['weightedratio'])
        temp['weight'] = temp['flag'] * temp['weightedratio'] / weight_hold_sum * weight_all_sum
        return temp[['s_info_windcode', 'weight']]

    def datamanage(self):
        t = time.time()
        # weight
        self.zz500_weight.reset_index(inplace=True)
        item = ['trade_dt', 's_info_windcode', 'induname1']
        self.data = pd.merge(self.zz500_weight, self.all_indu[item], how='left')
        self.data = pd.merge(self.data, self.factor, how='left')

        self.fac_name = self.factor.columns[-1]
        self.temp_weight = self.data.groupby(['trade_dt', 'induname1'])\
                                    .apply(self.weight_compute, self.fac_name, self.num)\
                                    .reset_index()
        item = ['trade_dt', 's_info_windcode', 'weight']
        self.data = pd.merge(self.data, self.temp_weight[item], how='left')
        self.data = pd.merge(self.data, self.all_data, how='left')
        print('weight data pre using time:%10.4fs' % (time.time() - t))

        t = time.time()
        # adjclosep
        self.data['adjclosep'] = self.data['s_dq_close'] * self.data['s_dq_adjfactor']
        item = ['trade_dt', 's_info_windcode', 'adjclosep']
        self.adjclosep = self.data[item].pivot('trade_dt', 's_info_windcode', 'adjclosep').copy()
        self.adjclosep.ffill(inplace=True)
        self.adjclosep.replace(np.nan, 0, inplace=True)
        # adjopenp
        self.data['adjopenp'] = self.data['s_dq_open'] * self.data['s_dq_adjfactor']
        item = ['trade_dt', 's_info_windcode', 'adjopenp']
        self.adjopenp = self.data[item].pivot('trade_dt', 's_info_windcode', 'adjopenp').copy()
        self.adjopenp.ffill(inplace=True)
        self.adjopenp.replace(np.nan, 0, inplace=True)
        # vol
        item = ['trade_dt', 's_info_windcode', 's_dq_volume']
        self.vol = self.data[item].pivot('trade_dt', 's_info_windcode', 's_dq_volume').copy()
        # self.vol.ffill(inplace=True)
        self.vol.replace(np.nan, 0, inplace=True)
        # weight
        item = ['trade_dt', 's_info_windcode', 'weight']
        self.weight = self.data[item].pivot('trade_dt', 's_info_windcode', 'weight').copy()
        # self.weight.ffill(inplace=True)
        self.weight.replace(np.nan, 0, inplace=True)
        print('basic data pre using time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据输出
        self.adjclosep.to_pickle(self.save_indir + 'all_adjclosep.pkl')
        self.adjopenp.to_pickle(self.save_indir + 'all_adjopenp.pkl')
        self.vol.to_pickle(self.save_indir + 'all_vol.pkl')
        self.weight.to_pickle(self.save_indir + self.weight_name)
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    zz500_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_combine\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name = ['all_dayindex.pkl', 'all_band_indu.pkl']
    zz500_name = 'zz500_freeweight.pkl'

    set = ['factor_combine_rankic1.pkl', 9, 'all_weight_1_9.pkl']
    twp = TradeWeightPre(file_indir, zz500_indir, factor_indir, save_indir, file_name, zz500_name, set)
    twp.runflow()
