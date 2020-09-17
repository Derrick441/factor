import pandas as pd
import numpy as np
import time


# 暂未作newey west调整
# 日频率
# 高频数据计算的：日内收益率波动率，日内收益率偏度，日内收益率峰度，日内成交量波动率，日内成交量偏度，日内成交量峰度和日内成交量HHI
class IntradaySevenIndex(object):

    def __init__(self, file_indir, file_name):
        self.file_indir = file_indir
        self.file_name = file_name

    def filein(self, file):
        t = time.time()
        # 从dataflow文件夹中取日内5分钟高频数据
        self.data = pd.read_pickle(self.file_indir + file)
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        pass

    def sevenindex(self, data):
        # t = time.time()
        rvol = np.sqrt(data['change'].var())
        rskew = data['change'].skew()
        rkurt = data['change'].kurt()
        vvol = np.sqrt(data['volume'].var())
        vskew = data['volume'].skew()
        vkurt = data['volume'].kurt()
        temp = data['volume']/data['volume'].sum()
        vhhi = temp.apply(lambda x: x ** 2).sum()
        # print(time.time()-t)
        return rvol, rskew, rkurt, vvol, vskew, vkurt, vhhi

    def compute_intraday7index(self):
        t = time.time()
        temp = self.data.groupby(['s_info_windcode', 'trade_dt']) \
                        .apply(self.sevenindex)\
                        .apply(pd.Series)\
                        .reset_index()
        self.temp_result = temp.rename(columns={0: 'rvol', 1: 'rskew', 2: 'rkurt',
                                                3: 'vvol', 4: 'vskew', 5: 'vkurt', 6: 'vhhi'})
        print('compute_intraday7index running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')

        # 分年度计算因子
        self.result_sum = []
        for i in self.file_name:
            print(i)
            self.filein(i)
            self.data_manage()
            self.compute_intraday7index()
            self.result_sum.append(self.temp_result)
        # 因子汇总
        self.result_temp = pd.concat(self.result_sum)
        # 数据对齐
        self.all_data = pd.read_pickle(self.file_indir + 'all_dayindex.pkl')
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.result_temp, how='left')

        # 7个因子分别输出到factor文件夹的stockfactor中
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        item1 = ['trade_dt', 's_info_windcode', 'rvol']
        item2 = ['trade_dt', 's_info_windcode', 'rskew']
        item3 = ['trade_dt', 's_info_windcode', 'rkurt']
        item4 = ['trade_dt', 's_info_windcode', 'vvol']
        item5 = ['trade_dt', 's_info_windcode', 'vskew']
        item6 = ['trade_dt', 's_info_windcode', 'vkurt']
        item7 = ['trade_dt', 's_info_windcode', 'vhhi']
        self.result[item1].to_pickle(indir_factor + 'factor_hq_rvol.pkl')
        self.result[item2].to_pickle(indir_factor + 'factor_hq_rskew.pkl')
        self.result[item3].to_pickle(indir_factor + 'factor_hq_rkurt.pkl')
        self.result[item4].to_pickle(indir_factor + 'factor_hq_vvol.pkl')
        self.result[item5].to_pickle(indir_factor + 'factor_hq_vskew.pkl')
        self.result[item6].to_pickle(indir_factor + 'factor_hq_vkurt.pkl')
        self.result[item7].to_pickle(indir_factor + 'factor_hq_vhhi.pkl')

        print('end running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name = ['all_store_hqdata_2012_5_derive.pkl', 'all_store_hqdata_2013_5_derive.pkl',
                 'all_store_hqdata_2014_5_derive.pkl', 'all_store_hqdata_2015_5_derive.pkl',
                 'all_store_hqdata_2016_5_derive.pkl', 'all_store_hqdata_2017_5_derive.pkl',
                 'all_store_hqdata_2018_5_derive.pkl', 'all_store_hqdata_2019_5_derive.pkl']
    df = IntradaySevenIndex(file_indir, file_name)
    df.runflow()

    # file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    # file_name = ['all_store_hqdata_2012_5_derive.pkl']
    # df = IntradaySevenIndex(file_indir, file_name)
    # df.runflow()
