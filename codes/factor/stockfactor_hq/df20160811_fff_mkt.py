import pandas as pd
import time


# 5分钟频率
# 市场因子：中证500收益率
class Mkt(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name
        print(self.file_name)

    def filein(self):
        t = time.time()
        self.mkt_data_1min = pd.read_pickle(self.file_indir + self.file_name)
        print('filein using time:%10.4fs' % (time.time() - t))

    def compose_five(self, data):
        print(data.iloc[0, 0]+'-'+data.iloc[0, 2])
        # 数据排序
        data1 = data.sort_values(by='bargaintime')
        # 索引
        num1 = len(data)
        num2 = num1-1
        head_list = list(range(0, num2, 5))
        tail_list = list(range(5, num2, 5))
        tail_list.append(num1)
        # 1分钟数据整合为5分钟数据
        result_list = []
        for i, j in zip(head_list, tail_list):
            info_list = []
            temp_data = data1.values[i:j, :]
            info_list.append(temp_data[0, 0])
            info_list.append(temp_data[0, 1])
            info_list.append(temp_data[0, 2])
            info_list.append(temp_data[0, 3])
            info_list.append(temp_data[0, 4])
            info_list.append(temp_data[:, 5].max())
            info_list.append(temp_data[:, 6].min())
            info_list.append(temp_data[-1, 7])
            info_list.append(temp_data[:, 8].sum())
            info_list.append(temp_data[:, 9].sum())
            result_list.append(info_list)
        # 整理
        result = pd.DataFrame(result_list)
        result.columns = data.columns
        return result

    def datamanage(self):
        t = time.time()
        self.mkt_data = self.mkt_data_1min.groupby(['s_info_windcode', 'trade_dt'])\
                                          .apply(self.compose_five)\
                                          .reset_index(drop=True)
        print('datamanage using time:%10.4fs' % (time.time() - t))

    def compute(self):
        t = time.time()
        self.mkt_data['mkt'] = (self.mkt_data.closeprice - self.mkt_data.openprice) / self.mkt_data.openprice * 100
        print('compute using time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        item = ['trade_dt', 'bargaintime', 'mkt']
        self.mkt_data[item].to_pickle(self.save_indir + 'factor_mkt_5min_' + self.file_name[19:23] + '.pkl')
        print('fileout using time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4f' % (time.time()-t))
        return self


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\famafrench_factor\\'
    file_names = ['zz500_store_hqdata_2012.pkl', 'zz500_store_hqdata_2013.pkl',
                  'zz500_store_hqdata_2014.pkl', 'zz500_store_hqdata_2015.pkl',
                  'zz500_store_hqdata_2016.pkl', 'zz500_store_hqdata_2017.pkl',
                  'zz500_store_hqdata_2018.pkl', 'zz500_store_hqdata_2019.pkl']

    for file_name in file_names:
        mkt = Mkt(file_indir, save_indir, file_name)
        mkt.runflow()
