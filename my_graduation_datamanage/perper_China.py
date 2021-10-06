import pandas as pd

file_dir = 'C:\\Users\\Administrator\\Desktop\\数据\\data3\\'
file = '造纸业经济数据.xls'

data = pd.read_excel(file_dir + file)

data.rename(columns={'Unnamed: 0': '指标'}, inplace=True)
data['指标'] = data['指标'].ffill()
data.drop('Unnamed: 1', axis=1, inplace=True)

data_ = data.groupby('指标').sum()
data_.to_excel(file_dir + '企业单位数.xlsx', index=False)