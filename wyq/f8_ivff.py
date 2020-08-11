import pandas as pd
import numpy as np
import time
import statsmodels.api as sm

#暂未完成
# class Ivff(object):
#
#     def __init__(self, indir, INDEX):
#         self.indir = indir
#         self.INDEX = INDEX
#
#     def filein(self):
#         t = time.time()
#         self.all_data = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_day_mv_pe_pb_turn_close.pkl')
#         print('filein running time:%10.4fs' % (time.time()-t))
#
#     def data_manage(self):
#         t = time.time()
#
#         print('data_manage running time:%10.4fs' % (time.time() - t))
#
#     def regress(self, data, y, x):
#         Y = data[y]
#         X = data[x]
#         X['intercept'] = 1
#         result = sm.OLS(Y, X).fit()
#         return result.resid
#
#     def compute_ivff(self):
#         t0 = time.time()
#         t = time.time()
#         # 提取年月标志
#         self.temp_data_sum['year_month'] = self.temp_data_sum['trade_dt'].apply(lambda x: x[:6])
#         print(time.time() - t)
#
#         t = time.time()
#         # 按月度数据对每股进行回归，计算回归残差，并将其作为调整后的换手率
#         temp = self.temp_data_sum.copy()
#         temp['turnover_adjusted'] = self.temp_data_sum.groupby(['year_month', 's_info_windcode'])\
#                                                       .apply(self.regress, 'ln_turnover', ['ln_mv'])\
#                                                       .reset_index()\
#                                                       .rename(columns={0: 'turnover_adjusted'})
#         print(time.time()-t)
#
#         t = time.time()
#         self.result = pd.merge(self.data_sum, temp[['trade_dt', 's_info_windcode', 'turnover_adjusted']], how='left')
#         print(time.time() - t)
#
#         print('compute_turnover_adjusted running time:%10.4fs' % (time.time() - t0))
#
#     def fileout(self):
#         t = time.time()
#         self.result[['trade_dt','s_info_windcode','turnover_adjusted']].to_pickle(self.indir + 'factor' + '/' + self.INDEX + '_turnover_adjusted.pkl')
#         print('fileout running time:%10.4fs' % (time.time()-t))
#
#     def runflow(self):
#         t = time.time()
#         print('compute start')
#         self.filein()
#         self.data_manage()
#         self.compute_ivff()
#         self.fileout()
#         print('compute finish, all running time:%10.4fs' % (time.time() - t))
#         return self
#
# if __name__ == '__main__':
#     indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
#     INDEX = 'all'
#     ivff = Ivff(indir, INDEX)
#     # ivff.runflow()
#     ivff.filein()
#     ivff.data_manage()
#     ivff.compute_ivff()
#     ivff.fileout()
