import pandas as pd
import numpy as np
import os

file_dir = 'C:\\Users\\Administrator\\Desktop\\数据\\data2\\'  # 数据存放地址
file_list = os.listdir(file_dir)  # 用以查看文件夹中数据文件名

# 18、19年情况
wp_2018_2019_0 = pd.read_excel(file_dir + '世界各国纸浆进出口2018_2019.xls')
wp_2018_2019_x = wp_2018_2019_0.copy()
wp_2018_2019_x = wp_2018_2019_x.rename(columns={'Unnamed: 0': '指标', 'Unnamed: 1': 'country'}).copy()
wp_2018_2019_x['指标'] = wp_2018_2019_x['指标'].ffill()
wp_2018_2019_x['country'] = wp_2018_2019_x['country'].ffill()
wp_2018_2019_x.replace({'俄罗斯联邦': '俄罗斯',
                        '捷克共和国': '捷克',
                        '多米尼加共和国': '多米尼加',
                        '阿拉伯联合酋长国': '阿联酋',
                        '北韩': '朝鲜',
                        '印尼': '印度尼西亚'}, inplace=True)

wp_2018_2019_x = wp_2018_2019_x.iloc[:-3, :]

wp_2018_2019_x1 = wp_2018_2019_x.groupby(['指标', 'country']).sum().sum(axis=1).reset_index()
imp_wp_a = wp_2018_2019_x1[wp_2018_2019_x1['指标'].str.contains('进口金额')].copy().rename(columns={0: 'amount'})
exp_wp_a = wp_2018_2019_x1[wp_2018_2019_x1['指标'].str.contains('出口金额')].copy().rename(columns={0: 'amount'})

imp_wp_a = imp_wp_a.sort_values(by='amount', ascending=False).reset_index(drop=True)
exp_wp_a = exp_wp_a.sort_values(by='amount', ascending=False).reset_index(drop=True)

# 计算国家份额占比和累积占比函数
def acc_p(data):
    imp_wp_a = data.copy()
    imp_wp_a['amount_p'] = imp_wp_a['amount'] / imp_wp_a['amount'].sum()
    imp_wp_a['amount_acc'] = imp_wp_a['amount'].cumsum()
    imp_wp_a['amount_acc_p'] = imp_wp_a['amount_acc'] / imp_wp_a['amount'].sum()
    return imp_wp_a


imp_wp_a_new = acc_p(imp_wp_a)
exp_wp_a_new = acc_p(exp_wp_a)


# 世界木浆进出口数据
wp_1992_1995_0 = pd.read_excel(file_dir + '世界纸浆进出口1992_1995.xls')
wp_1996_2010_0 = pd.read_excel(file_dir + '世界纸浆进出口1996_2010.xls')
wp_2011_2011_0 = pd.read_excel(file_dir + '世界纸浆进出口2011_2011.xls')
wp_2012_2017_0 = pd.read_excel(file_dir + '世界纸浆进出口2012_2017.xls')

wp_2016_2017_0 = pd.read_excel(file_dir + '世界纸浆进出口2016_2017.xls')

# 木浆进出口数据处理函数
def data_manage1(data):
    temp = data.rename(columns={'Unnamed: 0': '指标'}).copy()
    temp['指标'] = temp['指标'].ffill()
    temp = temp.iloc[:-3, :]
    temp.drop(['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3'], axis=1, inplace=True)
    result = temp.groupby('指标').sum()
    result.rename(index={'进口金额（美元）': 'RD_a', '出口金额（美元）': 'RS_a',
                         '进口净重': 'RD', '进口重量（千克）': 'RD',
                         '出口净重': 'RS', '出口重量（千克）': 'RS'}, inplace=True)
    return result


wp_1992_1995 = data_manage1(wp_1992_1995_0)
wp_1996_2010 = data_manage1(wp_1996_2010_0)
wp_2011_2011 = data_manage1(wp_2011_2011_0)
wp_2012_2017 = data_manage1(wp_2012_2017_0)
wp_2016_2017 = data_manage1(wp_2016_2017_0)
wp_2018_2019 = data_manage1(wp_2018_2019_0)

item = [wp_1992_1995, wp_1996_2010, wp_2011_2011, wp_2012_2017, wp_2016_2017, wp_2018_2019]
wp_data = pd.concat(item, axis=1, join='outer')
wp_data = wp_data.T

# 进出口价格
wp_data['Pi'] = wp_data['RD_a'] / wp_data['RD']
wp_data['Pe'] = wp_data['RS_a'] / wp_data['RS']

# GDP
temp = imp_wp_a_new.set_index(['country'])
imp19_country = temp.index[:19]
temp = exp_wp_a_new.set_index(['country'])
exp17_country = temp.index[:17]

imp19_GDP_0 = pd.read_excel(file_dir + 'imp19_GDP.xls')
exp17_GDP_0 = pd.read_excel(file_dir + 'exp17_GDP.xls')

imp19_GDP = pd.DataFrame(imp19_GDP_0.iloc[:-3, 2:].mean(), columns=['GDP_im'])
exp17_GDP = pd.DataFrame(exp17_GDP_0.iloc[:-3, 2:].mean(), columns=['GDP_ex'])

# 原材料价格
imp19_wood_1992_2010_0 = pd.read_excel(file_dir + 'imp19_wood_1992_2010.xls')
imp19_wood_2011_2011_0 = pd.read_excel(file_dir + 'imp19_wood_2011_2011.xls')
imp19_wood_2012_2017_0 = pd.read_excel(file_dir + 'imp19_wood_2012_2017.xls')
imp19_wood_2018_2019_0 = pd.read_excel(file_dir + 'imp19_wood_2018_2019.xls')

exp17_wood_1992_2010_0 = pd.read_excel(file_dir + 'exp17_wood_1992_2010.xls')
exp17_wood_2011_2011_0 = pd.read_excel(file_dir + 'exp17_wood_2011_2011.xls')
exp17_wood_2012_2017_0 = pd.read_excel(file_dir + 'exp17_wood_2012_2017.xls')
exp17_wood_2018_2019_0 = pd.read_excel(file_dir + 'exp17_wood_2018_2019.xls')

# 木浆进出口数据处理函数
def data_manage2(data):
    temp = data.rename(columns={'Unnamed: 0': '指标'}).copy()
    temp['指标'] = temp['指标'].ffill()
    temp = temp.iloc[:-3, :]
    temp.drop(['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3'], axis=1, inplace=True)
    result = temp.groupby('指标').sum()
    result.rename(index={'出口金额（美元）': 'wood_a',
                         '出口净重': 'wood_w', '出口重量（千克）': 'wood_w'}, inplace=True)
    return result


imp19_wood_1992_2010 = data_manage2(imp19_wood_1992_2010_0)
imp19_wood_2011_2011 = data_manage2(imp19_wood_2011_2011_0)
imp19_wood_2012_2017 = data_manage2(imp19_wood_2012_2017_0)
imp19_wood_2018_2019 = data_manage2(imp19_wood_2018_2019_0)

item = [imp19_wood_1992_2010, imp19_wood_2011_2011, imp19_wood_2012_2017, imp19_wood_2018_2019]
wood_data_im = pd.concat(item, axis=1, join='outer')
wood_data_im = wood_data_im.T

wood_data_im['Pw_im'] = wood_data_im['wood_a'] / wood_data_im['wood_w']

exp17_wood_1992_2010 = data_manage2(exp17_wood_1992_2010_0)
exp17_wood_2011_2011 = data_manage2(exp17_wood_2011_2011_0)
exp17_wood_2012_2017 = data_manage2(exp17_wood_2012_2017_0)
exp17_wood_2018_2019 = data_manage2(exp17_wood_2018_2019_0)

item = [exp17_wood_1992_2010, exp17_wood_2011_2011, exp17_wood_2012_2017, exp17_wood_2018_2019]
wood_data_ex = pd.concat(item, axis=1, join='outer')
wood_data_ex = wood_data_ex.T

wood_data_ex['Pw_ex'] = wood_data_ex['wood_a'] / wood_data_ex['wood_w']

# merge
data_all = pd.concat([wp_data, imp19_GDP, exp17_GDP, wood_data_im, wood_data_ex], axis=1).dropna()

item = ['RD_a', 'RS_a', 'wood_a', 'wood_w']
data_all = data_all.drop(item, axis=1).reset_index()
data_all.rename(columns={'index': 'year'}, inplace=True)
data_all.to_excel(file_dir + 'data2.xlsx', index=False)
