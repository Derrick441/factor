import pandas as pd
import time
import os


class IcSum(object):
    def __init__(self, file_indir, file_names, save_indir):
        self.file_indir = file_indir
        self.file_names = file_names
        self.save_indir = save_indir
        self.num = len(self.file_names)

    def data_readin_merge(self, day):
        for i in range(self.num):
            read_indir1 = self.file_indir + 'IC_' + self.file_names[i][:-4] + '_' + str(day) + '.pkl'
            read_indir2 = self.file_indir + 'IC_' + self.file_names[i][:-4] + '_' + str(day) + '_neutral' + '.pkl'
            if i == 0:
                table = pd.read_pickle(read_indir1)
                temp = pd.read_pickle(read_indir2)
                table = pd.merge(table, temp, how='outer')
            else:
                temp = pd.read_pickle(read_indir1)
                table = pd.merge(table, temp, how='outer')
                temp = pd.read_pickle(read_indir2)
                table = pd.merge(table, temp, how='outer')
        table['trade_dt'] = table['trade_dt'].apply(lambda x: int(x))
        table.set_index('trade_dt', inplace=True)
        return table

    def construct_five_table(self):
        t = time.time()
        self.table_ic_1 = self.data_readin_merge(1)
        self.table_ic_1.to_csv(self.save_indir + 'ic_all_1.csv')

        self.table_ic_5 = self.data_readin_merge(5)
        self.table_ic_5.to_csv(self.save_indir + 'ic_all_5.csv')

        self.table_ic_10 = self.data_readin_merge(10)
        self.table_ic_10.to_csv(self.save_indir + 'ic_all_10.csv')

        self.table_ic_20 = self.data_readin_merge(20)
        self.table_ic_20.to_csv(self.save_indir + 'ic_all_20.csv')

        self.table_ic_60 = self.data_readin_merge(60)
        self.table_ic_60.to_csv(self.save_indir + 'ic_all_60.csv')
        print('construct 5 tables using time:%10.4fs' % (time.time()-t))

    def data_select_merge(self, start, end):
        data = []
        data.append(self.table_ic_1[(self.table_ic_1.index >= start) & (self.table_ic_1.index <= end)].mean())
        data.append(self.table_ic_5[(self.table_ic_5.index >= start) & (self.table_ic_5.index <= end)].mean())
        data.append(self.table_ic_10[(self.table_ic_10.index >= start) & (self.table_ic_10.index <= end)].mean())
        data.append(self.table_ic_20[(self.table_ic_20.index >= start) & (self.table_ic_20.index <= end)].mean())
        data.append(self.table_ic_60[(self.table_ic_60.index >= start) & (self.table_ic_60.index <= end)].mean())
        result = pd.concat(data, axis=1)
        result.columns = ['ic_1', 'ic_5', 'ic_10', 'ic_20', 'ic_60']
        return result

    def construct_three_table(self):
        t = time.time()
        self.table_ic_2017 = self.data_select_merge(20170101, 20200101)
        self.table_ic_2017.to_csv(self.save_indir + 'ic_mean_2017.csv')

        self.table_ic_2012 = self.data_select_merge(20120101, 20200101)
        self.table_ic_2012.to_csv(self.save_indir + 'ic_mean_2012.csv')

        self.table_ic_2005 = self.data_select_merge(20050101, 20200101)
        self.table_ic_2005.to_csv(self.save_indir + 'ic_mean_2005.csv')
        print('construct 3 tables using time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.construct_five_table()
        self.construct_three_table()
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic_sum\\'
    file_names = os.listdir('D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\')

    icsum = IcSum(file_indir, file_names, save_indir)
    icsum.runflow()

    # file_names = ['factor_hq_apb1d.pkl', 'factor_price_bi.pkl', 'factor_price_ivff.pkl']
    # icsum = IcSum(file_indir, file_names, save_indir)
    # icsum.runflow()
