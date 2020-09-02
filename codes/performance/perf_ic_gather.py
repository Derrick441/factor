import pandas as pd
import os


# method:IC or rank_IC
def table5(method):
    # 输入地址
    indir_ic = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
    ics = os.listdir(indir_ic)
    indir = [indir_ic + i for i in ics]
    # 输出地址
    indir_ic_sum = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic_sum\\'
    indir_out = [indir_ic_sum + method + '_table' + i + '.pkl' for i in ['1', '5', '10', '20', '60']]
    # 数据读取和整合
    f_name = []
    ret_1 = []
    ret_5 = []
    ret_10 = []
    ret_20 = []
    ret_60 = []
    for i in indir:
        data = pd.read_pickle(i)
        f_name.append(data.columns.name)
        ret_1.append(data.iloc[:, 0])
        ret_5.append(data.iloc[:, 1])
        ret_10.append(data.iloc[:, 2])
        ret_20.append(data.iloc[:, 3])
        ret_60.append(data.iloc[:, 4])
    ret = [ret_1, ret_5, ret_10, ret_20, ret_60]
    # 数据调整和输出
    for i in range(5):
        temp = pd.concat(ret[i], axis=1)
        temp.columns = f_name
        temp.to_pickle(indir_out[i])
    return 0


def table3(method):
    # ic数据地址和factor文件名
    indir_ic = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
    ics = os.listdir(indir_ic)
    indir = [indir_ic + i for i in ics]
    # 输出地址
    indir_ic_sum = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic_sum\\'
    indir_out = [indir_ic_sum + method + '_mean_table' + i + '.csv' for i in ['07-19', '12-19', '17-19']]
    # 数据读取和整合
    f_name = []
    mean_07 = []
    mean_12 = []
    mean_17 = []
    for i in indir:
        data = pd.read_pickle(i)
        f_name.append(data.columns.name)
        data.reset_index(inplace=True)
        data.trade_dt = data.trade_dt.astype(int)

        data07 = data[data.trade_dt >= 20070101]
        mean_07.append(data07.mean())

        data12 = data[data.trade_dt >= 20120101]
        mean_12.append(data12.mean())

        data17 = data[data.trade_dt >= 20170101]
        mean_17.append(data17.mean())
    mean = [mean_07, mean_12, mean_17]
    for i in range(3):
        temp = pd.concat(mean[i], axis=1)
        temp.columns = f_name
        result = temp.T.drop('trade_dt', axis=1)
        result.to_csv(indir_out[i])
    return 0


if __name__ == '__main__':
    table5('IC')
    table3('IC')
