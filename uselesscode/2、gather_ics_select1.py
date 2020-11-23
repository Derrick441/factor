import pandas as pd
import time
import os


# ic汇集
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
            # 读入第一个ic
            if i == 0:
                temp_0 = pd.read_csv(file_indir + file_names[i]).iloc[:, 1:3]
                print(temp_0.columns[-1])
                print(len(temp_0.dropna()))
            # 读入余下因子，合并在第一个因子后面
            else:
                temp = pd.read_csv(file_indir + file_names[i]).iloc[:, 1:3]
                print(temp.columns[-1])
                print(len(temp.dropna()))
                temp_0 = pd.merge(temp_0, temp, how='left')
        print('files read in:%10.4fs' % (time.time()-t))
        return temp_0

    def filein(self):
        t = time.time()
        self.factor = self.factor_read(self.file_indir, self.file_names)
        print('filein running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.factor.to_pickle(self.save_indir + self.save_name)
        self.factor.to_csv(self.save_indir + self.save_name[:-4] + '.csv')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\select1_ic\\'

    set1 = set(os.listdir('D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\select1\\'))
    set1_part = [x[:-4] for x in set1]
    set2 = os.listdir('D:\\wuyq02\\develop\\python\\data\\performance\\ic\\')
    file_names0 = set([ic_name for ic_name in set2 if ic_name.split('_ic')[0] in set1_part])
    file_names0 = sorted(file_names0, reverse=True)

    file_names = [i for i in file_names0 if 'ic1.' in i]
    save_name = 'select_ic1.pkl'
    fa = FactorAll(file_indir, save_indir, file_names, save_name)
    fa.runflow()

    file_names = [i for i in file_names0 if 'ic5.' in i]
    save_name = 'select_ic5.pkl'
    fa = FactorAll(file_indir, save_indir, file_names, save_name)
    fa.runflow()

    file_names = [i for i in file_names0 if 'ic10.' in i]
    save_name = 'select_ic10.pkl'
    fa = FactorAll(file_indir, save_indir, file_names, save_name)
    fa.runflow()

    file_names = [i for i in file_names0 if 'ic20.' in i]
    save_name = 'select_ic20.pkl'
    fa = FactorAll(file_indir, save_indir, file_names, save_name)
    fa.runflow()

    file_names = [i for i in file_names0 if 'ic60.' in i]
    save_name = 'select_ic60.pkl'
    fa = FactorAll(file_indir, save_indir, file_names, save_name)
    fa.runflow()
