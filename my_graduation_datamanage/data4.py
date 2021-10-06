import pandas as pd
import numpy as np
import os

file_dir = 'C:\\Users\\Administrator\\Desktop\\数据\\data4\\'
file_list = os.listdir(file_dir)

list1 = ['世界贸易数据库-年度（分国家HS1992）.xls',
         '世界贸易数据库-年度（分国家HS1997）.xls',
         '世界贸易数据库-年度（分国家HS2002）.xls',
         '世界贸易数据库-年度（分国家HS2007）.xls']

list2 = ['世界贸易数据库-年度（分国家HS2011）.xls',
         '世界贸易数据库-年度（分国家HS2012）.xls']

list3 = ['世界贸易数据库-年度（分国家HS2016）.xls',
         '世界贸易数据库-年度（分国家HS2018）.xls']

data_list = []
data_r_list = []
for file in list1:
    temp_ = pd.read_excel(file_dir + file)
    temp = temp_.iloc[:-3, :].copy()
    temp.rename(columns={'Unnamed: 0': '指标', 'Unnamed: 1': '对象', 'Unnamed: 3': '类型'}, inplace=True)
    temp[['指标', '对象']] = temp[['指标', '对象']].ffill()
    temp['对象'] = temp['对象'].apply(lambda x: x.split('（')[0])
    item = ['指标', '类型', 'Unnamed: 2']
    pp = temp[~temp['类型'].str.contains('4707')].set_index('对象').fillna(0).drop(item, axis=1).groupby(level='对象').sum()
    pp = pp.rename(index={'俄罗斯联邦': '俄罗斯',
                          '捷克共和国': '捷克',
                          '多米尼加共和国': '多米尼加',
                          '阿拉伯联合酋长国': '阿联酋',
                          '北韩': '朝鲜',
                          '印尼': '印度尼西亚',
                          '中国大陆': '中国'})

    pp_r = temp[temp['类型'].str.contains('4706')].set_index('对象').fillna(0).drop(item, axis=1).groupby(level='对象').sum()
    pp_r = pp_r.rename(index={'俄罗斯联邦': '俄罗斯',
                              '捷克共和国': '捷克',
                              '多米尼加共和国': '多米尼加',
                              '阿拉伯联合酋长国': '阿联酋',
                              '北韩': '朝鲜',
                              '印尼': '印度尼西亚',
                              '中国大陆': '中国'})

    data_list.append(pp)
    data_r_list.append(pp_r)

data_all1 = pd.concat(data_list, join='outer', axis=1)
data_r_all1 = pd.concat(data_r_list, join='outer', axis=1)

data_list = []
data_r_list = []
for file in list2:
    temp_ = pd.read_excel(file_dir + file)
    temp = temp_.iloc[:-3, :].copy()
    temp.rename(columns={'Unnamed: 0': '指标', 'Unnamed: 1': '对象', 'Unnamed: 3': '类型'}, inplace=True)
    temp[['指标', '对象']] = temp[['指标', '对象']].ffill()
    temp['对象'] = temp['对象'].apply(lambda x: x.split('（')[0])
    item = ['指标', '类型', 'Unnamed: 2']
    pp_all = temp[temp['类型'].str.contains('木浆及其他纤维状')].set_index('对象').fillna(0).drop(item, axis=1)
    pp_rec = temp[temp['类型'].str.contains('4707')].set_index('对象').fillna(0).drop(item, axis=1)
    pp = pp_all - pp_rec
    pp = pp.rename(index={'俄罗斯联邦': '俄罗斯',
                          '捷克共和国': '捷克',
                          '多米尼加共和国': '多米尼加',
                          '阿拉伯联合酋长国': '阿联酋',
                          '北韩': '朝鲜',
                          '印尼': '印度尼西亚',
                          '中国大陆': '中国'})

    pp_r = temp[temp['类型'].str.contains('4706')].set_index('对象').fillna(0).drop(item, axis=1).groupby(level='对象').sum()
    pp_r = pp_r.rename(index={'俄罗斯联邦': '俄罗斯',
                              '捷克共和国': '捷克',
                              '多米尼加共和国': '多米尼加',
                              '阿拉伯联合酋长国': '阿联酋',
                              '北韩': '朝鲜',
                              '印尼': '印度尼西亚',
                              '中国大陆': '中国'})
    data_list.append(pp)
    data_r_list.append(pp_r)

data_all2 = pd.concat(data_list, join='outer', axis=1)
data_r_all2 = pd.concat(data_r_list, join='outer', axis=1)

data_list = []
data_r_list = []
for file in list3:
    temp_ = pd.read_excel(file_dir + file)
    temp = temp_.iloc[:-3, :].copy()
    temp.rename(columns={'Unnamed: 0': '指标', 'Unnamed: 1': '对象', 'Unnamed: 3': '类型'}, inplace=True)
    temp[['指标', '对象']] = temp[['指标', '对象']].ffill()
    temp['对象'] = temp['对象'].apply(lambda x: x.split('（')[0])
    item = ['指标', '类型', 'Unnamed: 2']
    pp_all = temp[temp['类型'].str.contains('木浆及其他纤维状')].set_index('对象').fillna(0).drop(item, axis=1)
    pp_rec = temp[temp['类型'].str.contains('4707')].set_index('对象').fillna(0).drop(item, axis=1)
    pp = pp_all - pp_rec
    pp = pp.rename(index={'俄罗斯联邦': '俄罗斯',
                          '捷克共和国': '捷克',
                          '多米尼加共和国': '多米尼加',
                          '阿拉伯联合酋长国': '阿联酋',
                          '北韩': '朝鲜',
                          '印尼': '印度尼西亚',
                          '中国大陆': '中国'})
    pp_r = temp[temp['类型'].str.contains('4706')].set_index('对象').fillna(0).drop(item, axis=1).groupby(level='对象').sum()
    pp_r = pp_r.rename(index={'俄罗斯联邦': '俄罗斯',
                              '捷克共和国': '捷克',
                              '多米尼加共和国': '多米尼加',
                              '阿拉伯联合酋长国': '阿联酋',
                              '北韩': '朝鲜',
                              '印尼': '印度尼西亚',
                              '中国大陆': '中国'})
    data_list.append(pp)
    data_r_list.append(pp_r)

data_all3 = pd.concat(data_list, join='outer', axis=1)
data_r_all3 = pd.concat(data_r_list, join='outer', axis=1)

data_all = pd.concat([data_all1, data_all2, data_all3], join='outer', axis=1)
data_r_all = pd.concat([data_r_all1, data_r_all2, data_r_all3], join='outer', axis=1)

data_all.iloc[0, -4:] = data_all.iloc[:, -4:].sum()
data_all = data_all.fillna(0)
data_pct = data_all / data_all.iloc[0, :]
data_all.to_excel(file_dir + '1992_2019世界各国纸浆进口总量.xlsx')
data_pct.to_excel(file_dir + '1992_2019世界纸浆进口各国占比.xlsx')

data_china = pd.concat([data_all.loc['中国'], data_r_all.loc['中国']], axis=1)
data_china.to_excel(file_dir + '1992_2019data_china.xlsx')

