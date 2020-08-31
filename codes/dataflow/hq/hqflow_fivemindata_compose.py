import pandas as pd
import time


# 由1分钟高频数据合成5分钟高频数据
class ComposeFiveMinData(object):

    def __init__(self, file_i, file_n):
        self.file_i = file_i
        self.file_n = file_n

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中取日内高频数据
        self.all_data = pd.read_pickle(self.file_i + self.file_n)
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
        self.result = self.all_data.groupby(['s_info_windcode', 'trade_dt'])\
                                   .apply(self.compose_one_five)\
                                   .reset_index(drop=True)
        print('compose running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result.to_pickle(self.file_i + self.file_n[0:21] + '_5.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('compute start')
        self.filein()
        self.datamanage()
        self.fileout()
        print('compute finish, all running time:%10.4fs' % (time.time() - t))
        return self


if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name = ['all_store_hqdata_2012.pkl', 'all_store_hqdata_2013.pkl', 'all_store_hqdata_2014.pkl',
                 'all_store_hqdata_2015.pkl', 'all_store_hqdata_2016.pkl', 'all_store_hqdata_2017.pkl',
                 'all_store_hqdata_2018.pkl', 'all_store_hqdata_2019.pkl', 'all_store_hqdata_2020.pkl']
    for i in file_name:
        print(i[-8:-4])
        fmd = ComposeFiveMinData(indir, i)
        fmd.runflow()
