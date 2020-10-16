import pandas as pd
import numpy as np
import time
import os
from sklearn.preprocessing import StandardScaler


# 将合并因子（中性化因子、IC加权合并），构成新因子
class FactorCombine(object):

    def __init__(self, file_indir1, file_indir2, save_indir, file_names):
        self.file_indir1 = file_indir1
        self.file_indir2 = file_indir2
        self.save_indir = save_indir
        self.file_names = file_names
        self.num = len(self.file_names)

    def factor_read(self, file_indir, file_names):
        t = time.time()
        for i in range(self.num):
            # 读入第一个因子
            if i == 0:
                temp_0 = pd.read_pickle(file_indir + file_names[i])
            # 读入余下因子，合并在第一个因子后面
            else:
                temp = pd.read_pickle(file_indir + file_names[i])
                temp_0 = pd.merge(temp_0, temp, how='left')
        # 日期数据类型转换为int
        # temp_0['trade_dt'] = temp_0['trade_dt'].apply(lambda x: int(x))
        temp_0.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        temp_0.replace(np.nan, 0, inplace=True)
        print('files read in:%10.4fs' % (time.time()-t))
        return temp_0

    def ic_read(self, day):
        t = time.time()
        for i in range(self.num):
            read_indir = self.file_indir2 + 'IC_' + self.file_names[i][8:-4] + '_' + str(day) + '_neutral' + '.pkl'
            if i == 0:
                table = pd.read_pickle(read_indir)
            else:
                temp = pd.read_pickle(read_indir)
                table = pd.merge(table, temp, how='outer')
        # 日期数据类型转换为int
        # table['trade_dt'] = table['trade_dt'].apply(lambda x: int(x))
        table.sort_values(by='trade_dt', inplace=True)
        table.replace(np.nan, 0, inplace=True)
        print('files read in:%10.4fs' % (time.time() - t))
        return table

    def filein(self):
        # 读入因子
        self.factor = self.factor_read(self.file_indir1, self.file_names)
        # 读入IC
        self.table_ic_1 = self.ic_read(1)
        self.table_ic_5 = self.ic_read(5)
        self.table_ic_10 = self.ic_read(10)
        self.table_ic_20 = self.ic_read(20)
        self.table_ic_60 = self.ic_read(60)

    def datamanage(self):
        # 因子标准化
        scaler = StandardScaler()
        temp = scaler.fit_transform(self.factor)
        # 转dataframe
        self.factor_std = pd.DataFrame(temp, index=self.factor.index, columns=self.factor.columns)
        self.factor_std.reset_index(inplace=True)

    def combine_ic(self, data_ic, data_fa, flag):
        t = time.time()
        temp_ic = data_ic.copy()
        temp_fa = data_fa.copy()
        item = ['trade_dt', 's_info_windcode']
        name = 'combine' + flag

        # IC：计算20移动均值、数据对齐
        temp_ic.set_index('trade_dt', inplace=True)
        ic_roll20 = temp_ic.rolling(20).mean()
        ic_roll20.reset_index(inplace=True)
        temp_ic_roll20 = pd.merge(temp_fa[item], ic_roll20, how='left')

        # 因子：IC加权、求和得到合并因子
        temp1 = temp_fa.set_index(item)
        temp2 = temp_ic_roll20.set_index(item)
        result = pd.DataFrame(np.array(temp1) * np.array(temp2), index=temp1.index)
        result.replace(np.nan, 0, inplace=True)
        result[name] = result.sum(axis=1)

        # 调整
        result.reset_index(inplace=True)
        # result['trade_dt'] = result['trade_dt'].apply(lambda x: str(x))
        print('combine using time:%10.4fs' % (time.time() - t))
        return result[['trade_dt', 's_info_windcode', name]]

    def compute(self):
        # 合并因子(以ic为权重）
        self.combine_factor_ic_1 = self.combine_ic(self.table_ic_1, self.factor_std, '_ic_1')
        self.combine_factor_ic_5 = self.combine_ic(self.table_ic_5, self.factor_std, '_ic_5')
        self.combine_factor_ic_10 = self.combine_ic(self.table_ic_10, self.factor_std, '_ic_10')
        self.combine_factor_ic_20 = self.combine_ic(self.table_ic_20, self.factor_std, '_ic_20')
        self.combine_factor_ic_60 = self.combine_ic(self.table_ic_60, self.factor_std, '_ic_60')

    def fileout(self):
        # 数据输出（因子之前已数据对齐，所以不再数据对齐）
        self.combine_factor_ic_1.to_pickle(self.save_indir + 'combine_factor_ic_1.pkl')
        self.combine_factor_ic_5.to_pickle(self.save_indir + 'combine_factor_ic_5.pkl')
        self.combine_factor_ic_10.to_pickle(self.save_indir + 'combine_factor_ic_10.pkl')
        self.combine_factor_ic_20.to_pickle(self.save_indir + 'combine_factor_ic_20.pkl')
        self.combine_factor_ic_60.to_pickle(self.save_indir + 'combine_factor_ic_60.pkl')

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir1 = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'
    file_indir2 = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_combine\\'
    file_names = os.listdir(file_indir1)
    
    fc = FactorCombine(file_indir1, file_indir2, save_indir, file_names)
    fc.runflow()
    
    # # 因子文件名
    # file_names = ['neutral_factor_hq_apb1d.pkl', 'neutral_factor_price_bi.pkl']
    # fc = FactorCombine(file_indir1, file_indir2, save_indir, file_names)
    # fc.runflow()
