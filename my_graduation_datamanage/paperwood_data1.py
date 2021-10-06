import pandas as pd
import numpy as np
import os

file_dir = 'C:\\Users\\Administrator\\Desktop\\数据\\data1\\'  # 数据存放地址
file_list = os.listdir(file_dir)  # 文件夹中数据文件名

# 中国木浆进口数据处理函数
def data_manage1(data):
    # data = pd.read_excel(file_dir + 'RD_1992_2001.xls')
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
    temp.drop(['Unnamed: 1', 'Unnamed: 3'], axis=1, inplace=True)

    ix = temp['指标'] == '进口金额（美元）'
    amount = temp[ix].drop(['指标'], axis=1).groupby(level='country').sum()
    ix = (temp['指标'] == '进口重量（千克）') | (temp['指标'] == '进口净重')
    weight = temp[ix].drop(['指标'], axis=1).groupby(level='country').sum()
    return amount, weight


# 中国木浆进口数据
RD_list = [x for x in file_list if 'RD' in x]
RD_amount_list = []
RD_weight_list = []
for file in RD_list:
    temp = pd.read_excel(file_dir + file)
    temp_amount, temp_weight = data_manage1(temp)
    RD_amount_list.append(temp_amount)
    RD_weight_list.append(temp_weight)

RD_amount = pd.concat(RD_amount_list, axis=1, join='outer')
RD_weight = pd.concat(RD_weight_list, axis=1, join='outer')

# 中国木片出口数据处理函数
def data_manage2(data):
    temp = data.rename(columns={'Unnamed: 0': '指标'}).copy()
    temp['指标'] = temp['指标'].ffill()
    temp = temp.iloc[:-3, :]
    temp.drop(['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3'], axis=1, inplace=True)
    amount = temp[temp['指标'] == '出口金额（美元）'].drop(['指标'], axis=1).reset_index(drop=True)
    weight = temp[(temp['指标'] == '出口重量（千克）') | (temp['指标'] == '出口净重')].drop(['指标'], axis=1).reset_index(drop=True)
    return amount, weight

# 中国木片出口数据
wood_list = [x for x in file_list if '中国出口木片' in x]
wood_amount_list = []
wood_weight_list = []
for file in wood_list:
    temp = pd.read_excel(file_dir + file)
    temp_amount, temp_weight = data_manage2(temp)
    wood_amount_list.append(temp_amount)
    wood_weight_list.append(temp_weight)

wood_amount = pd.concat(wood_amount_list, axis=1, join='outer')
wood_weight = pd.concat(wood_weight_list, axis=1, join='outer')

# 世界各国向中国出口木浆数据处理函数
def data_manage3(data):
    # data = pd.read_excel(file_dir + 'RS_1992_2001.xls')
    temp = data.rename(columns={'Unnamed: 0': '指标', 'Unnamed: 1': 'country', 'Unnamed: 2': '出口对象'}).copy()
    temp['country'] = temp['country'].ffill()
    temp['country'] = temp['country'].apply(lambda x: x.split('（')[0])
    temp['出口对象'] = temp['出口对象'].ffill()
    temp['指标'] = temp['指标'].ffill()

    temp.set_index(['country'], inplace=True)
    temp = temp.rename(index={'俄罗斯联邦': '俄罗斯',
                              '捷克共和国': '捷克',
                              '多米尼加共和国': '多米尼加',
                              '阿拉伯联合酋长国': '阿联酋',
                              '北韩': '朝鲜',
                              '印尼': '印度尼西亚'})

    temp_c = temp[(temp['出口对象'] == '中国') | (temp['出口对象'] == '中国大陆')].copy()
    temp_w = temp[temp['出口对象'] == '世界'].copy()

    temp_c.drop(['出口对象', 'Unnamed: 3'], axis=1, inplace=True)
    temp_w.drop(['出口对象', 'Unnamed: 3'], axis=1, inplace=True)

    ix = temp_c['指标'] == '出口金额（美元）'
    amount_c = temp_c[ix].drop(['指标'], axis=1).groupby(level='country').sum()
    ix = (temp_c['指标'] == '出口重量（千克）') | (temp_c['指标'] == '出口净重')
    weight_c = temp_c[ix].drop(['指标'], axis=1).groupby(level='country').sum()

    # ix = temp_w['指标'] == '出口金额（美元）'
    # amount_w = temp_w[ix].drop(['指标'], axis=1).groupby(level='country').sum()
    ix = (temp_w['指标'] == '出口重量（千克）') | (temp_w['指标'] == '出口净重')
    weight_w = temp_w[ix].drop(['指标'], axis=1).groupby(level='country').sum()
    return amount_c, weight_c, weight_w


# 世界各国向中国出口木浆数据
RS_list = [x for x in file_list if 'RS' in x]
RS_amount_list = []
RS_weight_list = []
RS_weight_w_list = []
for file in RS_list:
    temp = pd.read_excel(file_dir + file)
    temp_amount, temp_weight, temp_weight_w = data_manage3(temp)
    RS_amount_list.append(temp_amount)
    RS_weight_list.append(temp_weight)
    RS_weight_w_list.append(temp_weight_w)

RS_amount = pd.concat(RS_amount_list, axis=1, join='outer')
RS_weight = pd.concat(RS_weight_list, axis=1, join='outer')
RS_weight_w = pd.concat(RS_weight_w_list, axis=1, join='outer')

# 世界各国出口木屑价格处理函数
def data_manage4(data):
    temp = data.rename(columns={'Unnamed: 0': '指标', 'Unnamed: 1': 'country'}).copy()
    temp['指标'] = temp['指标'].ffill()
    temp['country'] = temp['country'].ffill()
    temp['country'] = temp['country'].apply(lambda x: x.split('（')[0])
    temp.set_index('country', inplace=True)

    temp = temp.iloc[:-3, :]
    temp.drop(['Unnamed: 2', 'Unnamed: 3'], axis=1, inplace=True)

    ix = temp['指标'] == '出口金额（美元）'
    amount = temp[ix].drop(['指标'], axis=1).groupby(level='country').sum().replace(0, np.nan)
    ix = (temp['指标'] == '出口重量（千克）') | (temp['指标'] == '出口净重')
    weight = temp[ix].drop(['指标'], axis=1).groupby(level='country').sum().replace(0, np.nan)
    return amount, weight


# 世界各国出口木屑价格
wwood_list = [x for x in file_list if '世界_世界_木屑' in x]
wwood_amount_list = []
wwood_weight_list = []
for file in wwood_list:
    temp = pd.read_excel(file_dir + file)
    temp_amount, temp_weight = data_manage4(temp)
    wwood_amount_list.append(temp_amount)
    wwood_weight_list.append(temp_weight)

wwood_amount = pd.concat(wwood_amount_list, axis=1, join='outer')
wwood_weight = pd.concat(wwood_weight_list, axis=1, join='outer')

# 世界GDP数据
GDP_0 = pd.read_excel(file_dir + 'GDP_1992_2020.xls')
temp = GDP_0.rename(columns={'Unnamed: 1': 'country'}).copy()
temp.set_index(['country'], inplace=True)
temp = temp.rename(index={'俄罗斯联邦': '俄罗斯',
                          '捷克共和国': '捷克',
                          '多米尼加共和国': '多米尼加',
                          '阿拉伯联合酋长国': '阿联酋',
                          '北韩': '朝鲜',
                          '印尼': '印度尼西亚'})
temp = temp.iloc[:-3, :]
GDP = temp.drop(['Unnamed: 0'], axis=1)

# 一阶差分
GDP = GDP.apply(lambda x: np.log(x/GDP.iloc[:, 0]*100))
GDP = GDP.T.diff(1).T

##########################################
# 进口需求
RD = RD_weight.iloc[1:, :].fillna(0).stack().reset_index()
RD.columns = ['country', 'time', 'RD']
RD.set_index(['country', 'time'], inplace=True)

# 进口金额
RD_a = RD_amount.iloc[1:, :].fillna(0).stack().reset_index()
RD_a.columns = ['country', 'time', 'RD_a']
RD_a.set_index(['country', 'time'], inplace=True)

# 中国从其它国家进口木浆数量
IMP_ = RD_weight.iloc[1:, :].apply(lambda x: RD_weight.iloc[0, :]-x, axis=1)
IMP = IMP_.fillna(0).stack().reset_index()
IMP.columns = ['country', 'time', 'IMP']
IMP.set_index(['country', 'time'], inplace=True)

# 中国木屑价格
wood_price = wood_amount.sum() / wood_weight.sum()
Pwi_ = RD_weight.iloc[1:, :].apply(lambda x: wood_price, axis=1)
# 一阶差分
Pwi_ = RD_weight.iloc[1:, :].apply(lambda x: np.log(wood_price), axis=1)
Pwi_ = Pwi_.T.diff(1).T
Pwi = Pwi_.fillna(0).stack().reset_index()
Pwi.columns = ['country', 'time', 'Pwi']
Pwi.set_index(['country', 'time'], inplace=True)
##########################################
# 纸浆出口国对中国的纸浆出口（剩余供给）
RS = RS_weight.fillna(0).stack().reset_index()
RS.columns = ['country', 'time', 'RS']
RS.set_index(['country', 'time'], inplace=True)

# 纸浆出口金额
RS_a = RS_amount.fillna(0).stack().reset_index()
RS_a.columns = ['country', 'time', 'RS_a']
RS_a.set_index(['country', 'time'], inplace=True)

# 纸浆出口国向中国以外国家出口纸浆量
EXP_ = RS_weight_w.loc[RS_weight.index] - RS_weight
EXP = EXP_.fillna(0).stack().reset_index()
EXP.columns = ['country', 'time', 'EXP']
EXP.set_index(['country', 'time'], inplace=True)

# 纸浆出口国木屑价格
Pwj_ = wwood_amount / wwood_weight
Pwj = Pwj_.fillna(0).stack().reset_index()
Pwj.columns = ['country', 'time', 'Pwj']
Pwj.set_index(['country', 'time'], inplace=True)
#####################################
# 中国GDP
GDP_i_ = RD_weight.iloc[1:, :].apply(lambda x: GDP.loc['中国'], axis=1)
GDP_i = GDP_i_.fillna(0).stack().reset_index()
GDP_i.columns = ['country', 'time', 'GDP_i']
GDP_i.set_index(['country', 'time'], inplace=True)

# 纸浆出口国的GDP
GDP_j = GDP.fillna(0).stack().reset_index()
GDP_j.columns = ['country', 'time', 'GDP_j']
GDP_j.set_index(['country', 'time'], inplace=True)
#########################################
# merge
data_all = pd.concat([RD, RD_a], axis=1, join='outer')
data_all = pd.concat([data_all, GDP_i], axis=1, join='outer')
data_all = pd.concat([data_all, Pwi], axis=1, join='outer')
data_all = pd.concat([data_all, IMP], axis=1, join='outer')
data_all = pd.concat([data_all, RS], axis=1, join='outer')
data_all = pd.concat([data_all, RS_a], axis=1, join='outer')
data_all = pd.concat([data_all, GDP_j], axis=1, join='inner')
data_all = pd.concat([data_all, Pwj], axis=1, join='inner')
data_all = pd.concat([data_all, EXP], axis=1, join='outer')
######################################
# 缺失值、异常值处理
A_result = data_all.copy().reset_index()

ix = (A_result['country'] == '美国') & (A_result['EXP'] < 0.0)
A_result.loc[ix, 'RS'] = A_result.loc[ix, 'RD']
ix_ = (A_result['country'] == '美国') & (A_result['EXP'] > 0.0)
A_result.loc[ix, 'EXP'] = A_result.loc[ix_, 'EXP'].mean()

ix = A_result['country'] == '挪威'
A_result.loc[ix, 'RS'] = A_result.loc[ix, 'RD']

A_result.iloc[:, :-1] = A_result.iloc[:, :-1].replace(0.0, np.nan)

A_result['Pi'] = A_result['RD_a'] / A_result['RD']
A_result['Pe'] = A_result['RS_a'] / A_result['RS']

num = 500000.0
A_result = A_result[(A_result['RD'] > num) & (A_result['RS'] > num)]
A_result = A_result[(A_result['RD_a'] > num) & (A_result['RS_a'] > num)]
A_result = A_result[(A_result['Pwi'] < 1.0) & (A_result['Pwj'] < 1.0)]
A_result = A_result[(A_result['Pi'] < 2.0) & (A_result['Pe'] < 2.0)]
A_result = A_result[~A_result['country'].str.contains('香港')]

A_result.drop(['RD_a', 'RS_a'], axis=1, inplace=True)
A_result = A_result.dropna()

A_result = A_result.sort_values(by=['country', 'time']).reset_index(drop=True)
A_result.to_excel(file_dir + 'data1.xlsx', index=False)

Countries = A_result.country.value_counts()
Countries_ = Countries[Countries >= 9].index
B_result = A_result[A_result.country.isin(Countries_)].reset_index(drop=True)
B_result.to_excel(file_dir + 'data1_1.xlsx', index=False)

C_result = B_result[B_result.time >= '2010'].reset_index(drop=True)
C_result.to_excel(file_dir + 'data1_2.xlsx', index=False)

Describe1 = B_result.describe()
Describe1.to_excel(file_dir + 'Describe1.xlsx')

# 查看所有国家
AA_country = RD.replace(0.0, np.nan).dropna().reset_index()
AA_country_ = AA_country.country.value_counts()

# 查看现有国家
BB_country = B_result.country.value_counts()

# 筛选主要六个国家
six_list = ['加拿大', '美国', '巴西', '智利', '印度尼西亚', '俄罗斯']
DATA_Six = A_result[A_result.country.isin(six_list)]
DATA_Six = DATA_Six[DATA_Six.time >= '2010']

DATA_SIX = DATA_Six.pivot(index='time', columns='country', values='Pi')
DATA_SIX.to_excel(file_dir + 'data6.xlsx')
