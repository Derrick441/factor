import pandas as pd
import time


# 由5分钟高频数据合成1小时频率数据
class ComposeOnehourData(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name
        print(self.file_name)

    def filein(self):
        t = time.time()
        # 取一年高频数据（5分钟）
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def compose_5min_1hour(self, data):
        temp = data.copy()
        # 数据显示
        print(temp.iloc[0, 0]+'-'+temp.iloc[0, 2])
        # 数据排序
        data1 = temp.sort_values(by='bargaintime')
        # 索引
        len1 = len(temp)
        len2 = len1-1
        head_list = list(range(0, len2, 12))
        tail_list = list(range(12, len2, 12))
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
        result.columns = temp.columns
        # 返回合并结果
        return result

    def compose(self):
        t = time.time()
        # 5分钟数据合成1小时
        self.result = self.all_data.groupby(['s_info_windcode', 'trade_dt'])\
                                   .apply(self.compose_5min_1hour)\
                                   .reset_index(drop=True)
        print('compose running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result.to_pickle(self.save_indir + self.file_name[:-4] + '_1h.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('compute start')
        self.filein()
        self.compose()
        self.fileout()
        print('compute finish, all running time:%10.4fs' % (time.time() - t))
        return self


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_names = ['all_store_hqdata_2012_5.pkl', 'all_store_hqdata_2013_5.pkl',
                  'all_store_hqdata_2014_5.pkl', 'all_store_hqdata_2015_5.pkl',
                  'all_store_hqdata_2016_5.pkl', 'all_store_hqdata_2017_5.pkl',
                  'all_store_hqdata_2018_5.pkl', 'all_store_hqdata_2019_5.pkl']

    for file_name in file_names:
        ohd = ComposeOnehourData(file_indir, save_indir, file_name)
        ohd.runflow()
