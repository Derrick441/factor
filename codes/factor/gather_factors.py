import pandas as pd
import numpy as np
import time
import os
from sklearn.preprocessing import StandardScaler


# 将合并因子（中性化因子、IC加权合并），构成新因子
class FactorAll(object):

    def __init__(self, file_indir, save_indir, file_names, save_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_names = file_names
        self.save_name = save_name

        self.num = len(self.file_names)
        print(self.num)

    def factor_read(self, file_indir, file_names):
        t = time.time()
        for i in range(self.num):
            # 读入第一个因子
            if i == 0:
                temp_0 = pd.read_pickle(file_indir + file_names[i])
                print(temp_0.columns[-1])
                print(len(temp_0.dropna()))
            # 读入余下因子，合并在第一个因子后面
            else:
                temp = pd.read_pickle(file_indir + file_names[i])
                print(temp.columns[-1])
                print(len(temp.dropna()))
                temp_0 = pd.merge(temp_0, temp, how='left')
        # # set_index
        # temp_0.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        print('files read in:%10.4fs' % (time.time()-t))
        return temp_0

    def filein(self):
        # 读入因子
        self.factor = self.factor_read(self.file_indir, self.file_names)

    def fileout(self):
        # 数据输出（因子之前已数据对齐，所以不再数据对齐）
        self.factor.to_pickle(self.save_indir + self.save_name)

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    # file_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_combine\\'
    file_names = os.listdir(file_indir)
    # save_name = 'factor_all.pkl'
    save_name = 'factor_neutral_all.pkl'

    fa = FactorAll(file_indir, save_indir, file_names, save_name)
    fa.runflow()
