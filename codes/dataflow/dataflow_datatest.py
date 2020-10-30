import pandas as pd

file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
file_name = 'all_dayindex.pkl'

all_data = pd.read_pickle(file_indir + file_name)
part_data = all_data[(all_data.trade_dt >= '20180101') & (all_data.trade_dt <= '20180630')]

part_data.to_pickle(file_indir + 'data_test.pkl')