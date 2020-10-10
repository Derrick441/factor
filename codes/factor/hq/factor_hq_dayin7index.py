import pandas as pd
import numpy as np
import time


# 暂未作newey west调整
# 高频数据计算的：日内收益率波动率，日内收益率偏度，日内收益率峰度，日内成交量波动率，日内成交量偏度，日内成交量峰度和日内成交量HHI
class DayinSevenIndex(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 输入数据
        self.data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        pass

    def sevenindex(self, data):
        rvol = np.sqrt(data['change'].var())
        rskew = data['change'].skew()
        rkurt = data['change'].kurt()
        vvol = np.sqrt(data['volume'].var())
        vskew = data['volume'].skew()
        vkurt = data['volume'].kurt()
        temp = data['volume']/data['volume'].sum()
        vhhi = temp.apply(lambda x: x ** 2).sum()
        return rvol, rskew, rkurt, vvol, vskew, vkurt, vhhi

    def compute(self):
        t = time.time()
        self.result = self.data.groupby(['s_info_windcode', 'trade_dt']) \
                               .apply(self.sevenindex)\
                               .apply(pd.Series)\
                               .reset_index()\
                               .rename(columns={0: 'rvol', 1: 'rskew', 2: 'rkurt',
                                                3: 'vvol', 4: 'vskew', 5: 'vkurt',
                                                6: 'vhhi'})
        print('compute_intraday7index running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据输出
        item1 = ['trade_dt', 's_info_windcode', 'rvol']
        self.result[item1].to_pickle(self.save_indir + 'factor_hq_rvol_' + self.file_name[17:21] + '.pkl')
        item2 = ['trade_dt', 's_info_windcode', 'rskew']
        self.result[item2].to_pickle(self.save_indir + 'factor_hq_rskew_' + self.file_name[17:21] + '.pkl')
        item3 = ['trade_dt', 's_info_windcode', 'rkurt']
        self.result[item3].to_pickle(self.save_indir + 'factor_hq_rkurt_' + self.file_name[17:21] + '.pkl')

        item4 = ['trade_dt', 's_info_windcode', 'vvol']
        self.result[item4].to_pickle(self.save_indir + 'factor_hq_vvol_' + self.file_name[17:21] + '.pkl')
        item5 = ['trade_dt', 's_info_windcode', 'vskew']
        self.result[item5].to_pickle(self.save_indir + 'factor_hq_vskew_' + self.file_name[17:21] + '.pkl')
        item6 = ['trade_dt', 's_info_windcode', 'vkurt']
        self.result[item6].to_pickle(self.save_indir + 'factor_hq_vkurt_' + self.file_name[17:21] + '.pkl')

        item7 = ['trade_dt', 's_info_windcode', 'vhhi']
        self.result[item7].to_pickle(self.save_indir + 'factor_hq_vhhi_' + self.file_name[17:21] + '.pkl')
        print('fileout using time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('end running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\minfactor\\'
    file_names = ['all_store_hqdata_2012_5_derive.pkl', 'all_store_hqdata_2013_5_derive.pkl',
                  'all_store_hqdata_2014_5_derive.pkl', 'all_store_hqdata_2015_5_derive.pkl',
                  'all_store_hqdata_2016_5_derive.pkl', 'all_store_hqdata_2017_5_derive.pkl',
                  'all_store_hqdata_2018_5_derive.pkl', 'all_store_hqdata_2019_5_derive.pkl']

    for file_name in file_names:
        print(file_name)
        dsi = DayinSevenIndex(file_indir, save_indir, file_name)
        dsi.runflow()

    def merge_data(factor_name, names):
        # 分开数据读取、合并
        indir1 = 'D:\\wuyq02\\develop\\python\\data\\factor\\minfactor\\'
        data_sum = []
        for name in names:
            data_sum.append(pd.read_pickle(indir1 + name))
        temp_result = pd.concat(data_sum)
        # 合并数据对齐、输出
        all_data = pd.read_pickle(file_indir + 'all_dayindex.pkl')
        result = pd.merge(all_data[['trade_dt', 's_info_windcode']], temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', factor_name]
        indir2 = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        result[item].to_pickle(indir2 + 'factor_hq_' + factor_name + '.pkl')

    factor_name = 'rvol'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'rskew'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'rkurt'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'vvol'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'vskew'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'vkurt'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'vhhi'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)
