import pandas as pd
import numpy as np
import time


class FactorX(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 股票日数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def overnight_return(self, data):
        temp = data.copy()
        temp['onr'] = (temp['s_dq_open'] - temp['s_dq_preclose']) / temp['s_dq_preclose'] * 100
        return pd.DataFrame({'trade_dt': temp.trade_dt.values, 'onr': temp['onr'].values})

    def datamanage(self):
        t = time.time()
        # 隔夜收益计算
        self.onr = self.all_data.groupby('s_info_windcode')\
                                .apply(self.overnight_return)\
                                .reset_index()
        # 数据合并
        self.data = pd.merge(self.all_data, self.onr, how='left')
        # 昨日换手率
        self.data['s_dq_freeturnover_y'] = self.data.groupby('s_info_windcode')['s_dq_freeturnover'].shift(1)
        # 去除空值
        self.data_dropna = self.data.dropna().copy()
        self.data_dropna = self.data_dropna[self.data_dropna.s_info_windcode < '000010.SZ']
        print('datamanage running time:%10.4fs' % (time.time() - t))

# #####################################################################################################################
    def method(self, data, perid, index_list):
        t = time.time()
        temp = data.copy()
        num = len(temp)
        if num > perid:
            intarday_part1 = [np.nan] * perid
            intarday_part5 = [np.nan] * perid
            overnight_part1 = [np.nan] * perid
            overnight_part5 = [np.nan] * perid
            for i in range(perid, num):
                temp_data = temp.iloc[i-perid:i, :].copy()
                temp_data['group'] = pd.cut(temp_data[index_list[0]].rank(), 5, labels=range(5))
                temp_mean = temp_data.groupby('group')[index_list[1]].mean()
                temp_data['group1'] = pd.cut(temp_data[index_list[2]].rank(), 5, labels=range(5))
                temp_mean1 = temp_data.groupby('group1')[index_list[3]].mean()
                intarday_part1.append(temp_mean[0])
                intarday_part5.append(temp_mean[4])
                overnight_part1.append(temp_mean1[0])
                overnight_part5.append(temp_mean1[4])
            result = pd.DataFrame({'trade_dt': temp.trade_dt.values,
                                   'intarday_part1': intarday_part1,
                                   'intarday_part5': intarday_part5,
                                   'overnight_part1': overnight_part1,
                                   'overnight_part5': overnight_part5})
            print(time.time() - t)
            return result
        else:
            result = pd.DataFrame({'trade_dt': temp.trade_dt.values,
                                   'intarday_part1': [None for i in range(num)],
                                   'intarday_part5': [None for i in range(num)],
                                   'overnight_part1': [None for i in range(num)],
                                   'overnight_part5': [None for i in range(num)]})
            return result

    def method1(self, data, index_list):
        temp = data.copy()
        result = (np.mean(temp[index_list[0]]), np.std(temp[index_list[0]]),
                  np.mean(temp[index_list[1]]), np.std(temp[index_list[1]]),
                  np.mean(temp[index_list[2]]), np.std(temp[index_list[2]]),
                  np.mean(temp[index_list[3]]), np.std(temp[index_list[3]]))
        return result

    def method2(self, data, index_list):
        temp = data.copy()
        result = (np.mean(temp[index_list[0]]), np.std(temp[index_list[0]]),
                  np.mean(temp[index_list[1]]), np.std(temp[index_list[1]]))
        return result

    def compute(self):
        t = time.time()
        # 因子计算
        item = ['s_dq_freeturnover', 's_dq_pctchange', 's_dq_freeturnover_y', 'onr']
        self.intarday_data = self.data_dropna.groupby('s_info_windcode')\
                                             .apply(self.method, 20, item)\
                                             .reset_index()
        item = ['intarday_part1', 'intarday_part5', 'overnight_part1', 'overnight_part5']
        self.temp_meanstd = self.intarday_data.groupby('trade_dt')\
                                              .apply(self.method1, item)\
                                              .apply(pd.Series)\
                                              .reset_index()\
                                              .rename(columns={0: 'intarday_part1_mean', 1: 'intarday_part1_std',
                                                               2: 'intarday_part5_mean', 3: 'intarday_part5_std',
                                                               4: 'overnight_part1_mean', 5: 'overnight_part1_std',
                                                               6: 'overnight_part5_mean', 7: 'overnight_part5_std'})
        self.temp_result = pd.merge(self.intarday_data, self.temp_meanstd, how='left')
        temp = self.temp_result.copy()
        self.temp_result['new_intarday1'] = ((temp.intarday_part1 - temp.intarday_part1_mean) / temp.intarday_part1_std)
        self.temp_result['new_intarday5'] = ((temp.intarday_part5 - temp.intarday_part5_mean) / temp.intarday_part5_std)
        self.temp_result['new_intarday'] = self.temp_result['new_intarday5'] - self.temp_result['new_intarday1']

        self.temp_result['new_overnight1'] = ((temp.overnight_part1-temp.overnight_part1_mean)/temp.overnight_part1_std)
        self.temp_result['new_overnight5'] = ((temp.overnight_part5-temp.overnight_part5_mean)/temp.overnight_part5_std)
        self.temp_result['new_overnight'] = self.temp_result['new_overnight1'] - self.temp_result['new_overnight5']

        item = ['new_intarday', 'new_overnight']
        self.temp_meanstd1 = self.temp_result.groupby('trade_dt') \
                                             .apply(self.method2, item) \
                                             .apply(pd.Series) \
                                             .reset_index() \
                                             .rename(columns={0: 'new_intarday_mean', 1: 'new_intarday_std',
                                                              2: 'new_overnight_mean', 3: 'new_overnight_std'})
        self.temp_result = pd.merge(self.temp_result, self.temp_meanstd1, how='left')
        temp = self.temp_result.copy()
        self.temp_result['new_intarday_f'] = ((temp.new_intarday-temp.new_intarday_mean)/temp.new_intarday_std)
        self.temp_result['new_overnight_f'] = ((temp.new_overnight-temp.new_overnight_mean)/temp.new_overnight_std)
        self.temp_result['nmom'] = self.temp_result['new_intarday_f'] - self.temp_result['new_overnight_f']
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'nmom']
        self.result[item].to_pickle(self.save_indir + 'factor_price_nmom.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        # self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    # file_name = 'data_test.pkl'
    file_name = 'all_dayindex.pkl'

    fx = FactorX(file_indir, save_indir, file_name)
    fx.runflow()
