import pandas as pd
import time


# 5分钟频率
# 市场因子：中证500收益率
class Mkt(object):

    def __init__(self, indir, index):
        self.indir = indir
        self.index = index

    def filein(self):
        t = time.time()
        self.mkt_data_row = pd.read_pickle(self.indir + self.index)
        print('filein running time:%10.4fs' % (time.time() - t))

    def compose_one_five(self, data):
        # 显示
        print(data.iloc[0, 0]+'-'+data.iloc[0, 2])
        # 数据排序（9.30->15.00）
        data1 = data.sort_values(by='bargaintime')
        # 索引
        len1 = len(data)
        len2 = len1-1
        head_list = list(range(0, len2, 5))
        tail_list = list(range(5, len2, 5))
        tail_list.append(len1)
        # 合并数据存放空间
        result_list = []
        # 合并
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
        # 格式调整
        result = pd.DataFrame(result_list)
        result.columns = data.columns
        # 返回合并结果
        return result

    def datamanage(self):
        t = time.time()
        # 一分钟数据合并成5分钟数据
        self.mkt_data = self.mkt_data_row.groupby(['s_info_windcode', 'trade_dt'])\
                                         .apply(self.compose_one_five)\
                                         .reset_index(drop=True)
        print('compose running time:%10.4fs' % (time.time() - t))

    def mkt(self):
        t = time.time()
        # 每5分钟市场因子
        self.mkt_data['mkt'] = (self.mkt_data.closeprice - self.mkt_data.openprice) / self.mkt_data.openprice * 100
        # 结果
        item = ['trade_dt', 'bargaintime', 'mkt']
        self.result = self.mkt_data[item]
        print('mkt running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 存在factor文件夹的basicfactor中
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\mktfactor\\'
        self.result.to_pickle(indir_factor + 'factor_mkt_5min_' + self.index[19:23] + '.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('compute start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.mkt()
        self.fileout()
        print('compute end, running time:%10.4f' % (time.time()-t))
        return self


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\'
    file_index = ['zz500_store_hqdata_2012.pkl', 'zz500_store_hqdata_2013.pkl', 'zz500_store_hqdata_2014.pkl',
                  'zz500_store_hqdata_2015.pkl', 'zz500_store_hqdata_2016.pkl', 'zz500_store_hqdata_2017.pkl',
                  'zz500_store_hqdata_2018.pkl', 'zz500_store_hqdata_2019.pkl', 'zz500_store_hqdata_2020.pkl']
    for i in file_index:
        print(i[19:23])
        mkt = Mkt(file_indir, i)
        mkt.runflow()
