import pandas as pd
import numpy as np
import os

file_dir = 'C:\\Users\\Administrator\\Desktop\\数据\\data5\\'  # 数据存放地址
file_list = os.listdir(file_dir)  # 文件夹中数据文件名


def data_manage1(data):
    # data = pd.read_excel(file_dir + '2011-2011.xls')
    temp = data.rename(columns={'Unnamed: 0': '指标', 'Unnamed: 1': 'country', 'Unnamed: 3': 'product'}).copy()
    temp['指标'] = temp['指标'].ffill()
    temp['country'] = temp['country'].ffill()
    temp.set_index(['指标', 'country'], inplace=True)

    temp.drop(['Unnamed: 2'], axis=1, inplace=True)
    temp = temp.iloc[:-3, :]

    temp1 = temp[temp['product'].str.contains('4707')]
    temp2 = temp[~temp['product'].str.contains('4707')]
    temp3 = (temp2.iloc[:, 1:] - temp1.iloc[:, 1:]).reset_index()

    res1 = temp3[temp3['指标'].str.contains('金额')].set_index(['指标', 'country'])
    res2 = temp3[temp3['指标'].str.contains('重量')].set_index(['指标', 'country'])
    return res1, res2


Data_list = ['2011-2011.xls', '2012-2015.xls']
amount_list = []
weight_list = []
for file in Data_list:
    temp = pd.read_excel(file_dir + file)
    temp_amount, temp_weight = data_manage1(temp)
    amount_list.append(temp_amount)
    weight_list.append(temp_weight)

amount1 = pd.concat(amount_list, axis=1, join='outer').reset_index().drop(['指标'], axis=1).set_index(['country'])
weight1 = pd.concat(weight_list, axis=1, join='outer').reset_index().drop(['指标'], axis=1).set_index(['country'])

result_1 = amount1 / weight1

def data_manage2(data):
    data = pd.read_excel(file_dir + '2018-2019.xls')
    temp = data.rename(columns={'Unnamed: 0': '指标', 'Unnamed: 1': 'country', 'Unnamed: 3': 'product'}).copy()
    temp['指标'] = temp['指标'].ffill()
    temp['country'] = temp['country'].ffill()
    temp['country'] = temp['country'].apply(lambda x: x.split('（')[0])
    temp.set_index(['country'], inplace=True)
    temp = temp.rename(index={'俄罗斯联邦': '俄罗斯',
                              '捷克共和国': '捷克',
                              '多米尼加共和国': '多米尼加',
                              '阿拉伯联合酋长国': '阿联酋',
                              '北韩': '朝鲜',
                              '印尼': '印度尼西亚',
                              '中国大陆': '中国'})
    temp = temp.iloc[:-3, :]
    temp.drop(['Unnamed: 2'], axis=1, inplace=True)
    temp.fillna(0, inplace=True)
    china = temp.loc['中国']
    usa = temp.loc['美国']

    temp0_ = temp.groupby(['指标', 'product']).sum().reset_index()
    temp0_['世界'] = ['世界'] * 4
    temp0_.set_index(['世界'], inplace=True)

    temp0 = pd.concat([temp0_, china, usa]).reset_index().set_index(['index', '指标'])

    temp1 = temp0[temp0['product'].str.contains('4707')]
    temp2 = temp0[~temp0['product'].str.contains('4707')]
    temp3 = (temp2.iloc[:, 1:] - temp1.iloc[:, 1:]).reset_index()
    temp3.rename(columns={'index': 'country'}, inplace=True)

    res1 = temp3[temp3['指标'].str.contains('金额')].set_index(['指标', 'country'])
    res2 = temp3[temp3['指标'].str.contains('重量|数量')].set_index(['指标', 'country'])
    res2 = res2.rename(index={'进口数量': '进口重量（千克）'})
    return res1, res2


Data_list = ['2016-2017.xls', '2018-2019.xls']
amount_list = []
weight_list = []
for file in Data_list:
    temp = pd.read_excel(file_dir + file)
    temp_amount, temp_weight = data_manage2(temp)
    amount_list.append(temp_amount)
    weight_list.append(temp_weight)

amount2 = pd.concat(amount_list, axis=1, join='outer').reset_index().drop(['指标'], axis=1).set_index(['country'])
weight2 = pd.concat(weight_list, axis=1, join='outer').reset_index().drop(['指标'], axis=1).set_index(['country'])

result_2 = amount2 / weight2

data = pd.read_excel(file_dir + '1992-2010.xls')
temp = data.rename(columns={'Unnamed: 0': '指标', 'Unnamed: 1': 'country'}).copy()
temp['指标'] = temp['指标'].ffill()
temp['country'] = temp['country'].ffill()
temp['country'] = temp['country'].apply(lambda x: x.split('（')[0])
temp.drop(['Unnamed: 2', 'Unnamed: 3'], axis=1, inplace=True)
temp = temp.iloc[:-3, :]
temp3 = temp.groupby(['指标', 'country']).sum().reset_index()

amount3 = temp3[temp3['指标'].str.contains('金额')].drop(['指标'], axis=1).set_index(['country'])
weight3 = temp3[temp3['指标'].str.contains('重|数量')].drop(['指标'], axis=1).set_index(['country'])

result_3 = amount3 / weight3

A_result = pd.concat([result_3, result_1, result_2], axis=1)
A_result = A_result.T.rename(columns={'世界': 'world', '中国': 'china', '美国': 'usa'})
# A_result.plot.bar()
A_result.to_excel(file_dir + '1992_2019世界、中国和美国纸浆进口价.xlsx', index=False)