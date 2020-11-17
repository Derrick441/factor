import pandas as pd
import time
import os


# 因子汇集
class FactorAll(object):

    def __init__(self, file_indir, save_indir, file_names, save_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_names = file_names
        self.save_name = save_name

        self.num = len(self.file_names)
        print(self.num)

    def factor_read(self, file_indir, file_names):
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
        return temp_0

    def filein(self):
        t = time.time()
        self.factor = self.factor_read(self.file_indir, self.file_names)
        print('filein running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.factor.to_pickle(self.save_indir + self.save_name)
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    # file_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    # save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\'
    # file_names = os.listdir(file_indir)
    # file_names = sorted(file_names, reverse=True)
    # save_name = 'factor_all.pkl'
    #
    # fa = FactorAll(file_indir, save_indir, file_names, save_name)
    # fa.runflow()

    file_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\select2\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\'
    file_names = os.listdir(file_indir)
    file_names = sorted(file_names, reverse=True)
    save_name = 'select2.pkl'

    fa = FactorAll(file_indir, save_indir, file_names, save_name)
    fa.runflow()
