import pandas as pd
import numpy as np
import time
import os


class IcSum(object):
    def __init__(self, file_indir, file_names, save_indir, method):
        self.file_indir = file_indir
        self.file_names = file_names
        self.save_indir = save_indir
        self.method = method
        self.num = len(self.file_names)

    def ic_readin(self, day):
        for i in range(self.num):
            read_indir1 = self.file_indir + self.file_names[i][:-4] + '_' + self.method + str(day) + '.csv'
            if i == 0:
                table = pd.read_csv(read_indir1)
                table.drop(['Unnamed: 0', 'accu'], axis=1, inplace=True)
            else:
                temp = pd.read_csv(read_indir1)
                temp.drop(['Unnamed: 0', 'accu'], axis=1, inplace=True)
                table = pd.merge(table, temp, how='outer')
        table['trade_dt'] = table['trade_dt'].apply(lambda x: int(x))
        table.set_index('trade_dt', inplace=True)
        table.sort_values('trade_dt', inplace=True)
        return table

    def filein(self):
        t = time.time()
        self.table_ic_1 = self.ic_readin(1)
        self.table_ic_5 = self.ic_readin(5)
        self.table_ic_10 = self.ic_readin(10)
        self.table_ic_20 = self.ic_readin(20)
        self.table_ic_60 = self.ic_readin(60)
        print('construct 5 tables using time:%10.4fs' % (time.time()-t))

    def compute_ic(self, start, end):
        data = []

        temp_data = self.table_ic_1[(self.table_ic_1.index >= start) & (self.table_ic_1.index <= end)].copy()
        data.append(temp_data.mean())
        temp_data = self.table_ic_5[(self.table_ic_5.index >= start) & (self.table_ic_5.index <= end)].copy()
        data.append(temp_data.mean())
        temp_data = self.table_ic_10[(self.table_ic_10.index >= start) & (self.table_ic_10.index <= end)].copy()
        data.append(temp_data.mean())
        temp_data = self.table_ic_20[(self.table_ic_20.index >= start) & (self.table_ic_20.index <= end)].copy()
        data.append(temp_data.mean())
        temp_data = self.table_ic_60[(self.table_ic_60.index >= start) & (self.table_ic_60.index <= end)].copy()
        data.append(temp_data.mean())

        result = pd.concat(data, axis=1)
        m = self.method
        result.columns = [m + '_1', m + '_5', m + '_10', m + '_20', m + '_60']

        return result

    def compute_ir(self, start, end):
        data = []

        temp_data = self.table_ic_1[(self.table_ic_1.index >= start) & (self.table_ic_1.index <= end)].copy()
        data.append(temp_data.mean() / temp_data.std() * np.sqrt(240))
        temp_data = self.table_ic_5[(self.table_ic_5.index >= start) & (self.table_ic_5.index <= end)].copy()
        data.append(temp_data.mean() / temp_data.std() * np.sqrt(48))
        temp_data = self.table_ic_10[(self.table_ic_10.index >= start) & (self.table_ic_10.index <= end)].copy()
        data.append(temp_data.mean() / temp_data.std() * np.sqrt(24))
        temp_data = self.table_ic_20[(self.table_ic_20.index >= start) & (self.table_ic_20.index <= end)].copy()
        data.append(temp_data.mean() / temp_data.std() * np.sqrt(12))
        temp_data = self.table_ic_60[(self.table_ic_60.index >= start) & (self.table_ic_60.index <= end)].copy()
        data.append(temp_data.mean() / temp_data.std() * np.sqrt(4))

        result = pd.concat(data, axis=1)
        m = self.method
        result.columns = [m + 'ir_1', m + 'ir_5', m + 'ir_10', m + 'ir_20', m + 'ir_60']

        return result

    def construct_three_table(self):
        t = time.time()
        self.table_ic_2017 = self.compute_ic(20170101, 20200101)
        self.table_ic_2017.to_csv(self.save_indir + 'all_' + self.method + '_mean_2017.csv')
        self.table_ir_2017 = self.compute_ir(20170101, 20200101)
        self.table_ir_2017.to_csv(self.save_indir + 'all_' + self.method + 'ir_2017.csv')

        self.table_ic_2012 = self.compute_ic(20120101, 20200101)
        self.table_ic_2012.to_csv(self.save_indir + 'all_' + self.method + '_mean_2012.csv')
        self.table_ir_2012 = self.compute_ir(20120101, 20200101)
        self.table_ir_2012.to_csv(self.save_indir + 'all_' + self.method + 'ir_2012.csv')

        self.table_ic_2005 = self.compute_ic(20050101, 20200101)
        self.table_ic_2005.to_csv(self.save_indir + 'all_' + self.method + '_mean_2005.csv')
        self.table_ir_2005 = self.compute_ir(20050101, 20200101)
        self.table_ir_2005.to_csv(self.save_indir + 'all_' + self.method + 'ir_2005.csv')
        print('construct 3 tables using time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.construct_three_table()
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic_neutral\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic_sum_neutral\\'
    temp = os.listdir('D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\')
    file_names = [name for name in temp if 'pkl' in name]
    method = 'ic'

    icsum = IcSum(file_indir, file_names, save_indir, method)
    icsum.runflow()
