import pandas as pd
import numpy as np
import os

file_dir = 'C:\\Users\\Administrator\\Desktop\\数据\\data3\\'
file_list = os.listdir(file_dir)

# 中国木浆进口数据处理函数
def data_manage1(data):
    # data = pd.read_excel(file_dir + '世界贸易数据库-年度（分国家HS2017）.xls')
    temp = data.rename(columns={'Unnamed: 0': '指标', 'Unnamed: 2': 'country'}).copy()
    temp['指标'] = temp['指标'].ffill()
    temp['country'] = temp['country'].ffill()
    temp['country'] = temp['country'].apply(lambda x: x.split('（')[0])
    temp.set_index(['country'], inplace=True)
    temp = temp.rename(index={'俄罗斯联邦': '俄罗斯',
                              '捷克共和国': '捷克',
                              '多米尼加共和国': '多米尼加',
                              '阿拉伯联合酋长国': '阿联酋',
                              '北韩': '朝鲜',
                              '印尼': '印度尼西亚'})

    temp = temp.iloc[:-3, :]
    temp.drop(['Unnamed: 1'], axis=1, inplace=True)

    temp_1 = temp[temp['Unnamed: 3'].str.contains('47 - 木浆及')].copy().drop(['Unnamed: 3'], axis=1)
    temp_2 = temp[temp['Unnamed: 3'].str.contains('4707')].copy().drop(['Unnamed: 3'], axis=1)
    # temp_2 = temp[temp['Unnamed: 3'].str.contains('4707|4706')].copy().drop(['Unnamed: 3'], axis=1)

    ix = temp_1['指标'] == '进口金额（美元）'
    amount_1 = temp_1[ix].drop(['指标'], axis=1).groupby(level='country').sum()
    ix = (temp_1['指标'] == '进口重量（千克）') | (temp_1['指标'] == '进口净重')
    weight_1 = temp_1[ix].drop(['指标'], axis=1).groupby(level='country').sum()

    ix = temp_2['指标'] == '进口金额（美元）'
    amount_2 = temp_2[ix].drop(['指标'], axis=1).groupby(level='country').sum()
    ix = (temp_2['指标'] == '进口重量（千克）') | (temp_2['指标'] == '进口净重')
    weight_2 = temp_2[ix].drop(['指标'], axis=1).groupby(level='country').sum()
    return amount_1, weight_1, amount_2, weight_2


# 中国纸浆进口数据
data_1217 = pd.read_excel(file_dir + '世界贸易数据库-年度（分国家HS2012）.xls')
data_1819 = pd.read_excel(file_dir + '世界贸易数据库-年度（分国家HS2017）.xls')

amount_1_1217, weight_1_1217, amount_2_1217, weight_2_1217 = data_manage1(data_1217)
amount_1_1819, weight_1_1819, amount_2_1819, weight_2_1819 = data_manage1(data_1819)

# amount_1 = pd.concat([amount_1_1217, amount_1_1819], axis=1, join='outer')
weight_1 = pd.concat([weight_1_1217, weight_1_1819], axis=1, join='outer')
# amount_2 = pd.concat([amount_2_1217, amount_2_1819], axis=1, join='outer')
weight_2 = pd.concat([weight_2_1217, weight_2_1819], axis=1, join='outer')

weight1_1219 = weight_1.sum()
weight2_1219 = weight_2.sum()
weight_1219 = weight_1.sum() - weight_2.sum()

# weight_1219.to_excel(file_dir + '中国纸浆进口量_2012_2019.xlsx', index=False)
# weight_1219.to_excel(file_dir + '中国木浆浆进口量_2012_2019.xlsx', index=False)

# 计算国家份额占比和累积占比函数
def acc_p(data, index):
    temp = data.copy()
    temp[index + '_p'] = temp[index] / temp[index].sum()
    temp[index + '_acc'] = temp[index].cumsum()
    temp[index + '_acc_p'] = temp[index + '_acc'] / temp[index].sum()
    return temp


weight_ = (weight_1['2019'] - weight_2['2019']).replace(0, np.nan).dropna().reset_index().rename(columns={'2019': 'weight'})
weight_temp = weight_.sort_values(by='weight', ascending=False).reset_index(drop=True)
compose_2019 = acc_p(weight_temp, 'weight')

weight_ = (weight_1['2012'] - weight_2['2012']).replace(0, np.nan).dropna().reset_index().rename(columns={'2012': 'weight'})
weight_temp = weight_.sort_values(by='weight', ascending=False).reset_index(drop=True)
compose_2012 = acc_p(weight_temp, 'weight')

weight_ = (weight_1['2016'] - weight_2['2016']).replace(0, np.nan).dropna().reset_index().rename(columns={'2016': 'weight'})
weight_temp = weight_.sort_values(by='weight', ascending=False).reset_index(drop=True)
compose_2016 = acc_p(weight_temp, 'weight')

compose_2012.to_excel(file_dir + 'compose_2012.xlsx', index=False)
compose_2016.to_excel(file_dir + 'compose_2016.xlsx', index=False)
compose_2019.to_excel(file_dir + 'compose_2019.xlsx', index=False)


# 纸浆进口量
Aweight = weight_1 - weight_2
Aweight_ = Aweight.replace(0.0, np.nan).dropna(how='all').reset_index()

Aweight_.to_excel(file_dir + '2012_2018纸浆进口量.xlsx', index=False)
