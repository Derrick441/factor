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

wp_2018_2019_x1 = wp_2018_2019_x.groupby(['指标', 'country']).sum().drop('2018', axis=1).reset_index()
imp_wp_a = wp_2018_2019_x1[wp_2018_2019_x1['指标'].str.contains('进口金额')].copy().rename(columns={'2019': 'amount'})
imp_wp_w = wp_2018_2019_x1[wp_2018_2019_x1['指标'].str.contains('进口净重')].copy().rename(columns={'2019': 'weight'})
exp_wp_a = wp_2018_2019_x1[wp_2018_2019_x1['指标'].str.contains('出口金额')].copy().rename(columns={'2019': 'amount'})
exp_wp_w = wp_2018_2019_x1[wp_2018_2019_x1['指标'].str.contains('出口净重')].copy().rename(columns={'2019': 'weight'})

imp_wp_a = imp_wp_a.sort_values(by='amount', ascending=False).reset_index(drop=True)
imp_wp_w = imp_wp_w.sort_values(by='weight', ascending=False).reset_index(drop=True)
exp_wp_a = exp_wp_a.sort_values(by='amount', ascending=False).reset_index(drop=True)
exp_wp_w = exp_wp_w.sort_values(by='weight', ascending=False).reset_index(drop=True)

# 计算国家份额占比和累积占比函数
def acc_p(data, index):
    temp = data.copy()
    temp[index + '_p'] = temp[index] / temp[index].sum()
    temp[index + '_acc'] = temp[index].cumsum()
    temp[index + '_acc_p'] = temp[index + '_acc'] / temp[index].sum()
    return temp


imp_wp_a_new = acc_p(imp_wp_a, 'amount')
exp_wp_a_new = acc_p(exp_wp_a, 'amount')

imp_wp_w_new = acc_p(imp_wp_w, 'weight')
exp_wp_w_new = acc_p(exp_wp_w, 'weight')

imp_wp_a_new.to_excel(file_dir + '1、世界进口金额数据.xlsx', index=False)
imp_wp_w_new.to_excel(file_dir + '2、世界进口净重数据.xlsx', index=False)
exp_wp_a_new.to_excel(file_dir + '3、世界出口金额数据.xlsx', index=False)
exp_wp_w_new.to_excel(file_dir + '4、世界出口净重数据.xlsx', index=False)
