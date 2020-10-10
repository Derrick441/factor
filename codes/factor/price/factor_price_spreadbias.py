import pandas as pd
import numpy as np
import time
np.seterr(invalid='ignore')


# 价差偏离度：股价与参考价格的对数价差的偏离度
class Spreadbias(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 股票日行情数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 取股价、涨幅数据
        self.price = self.all_data[['trade_dt', 's_info_windcode', 's_dq_close_today']].copy()
        self.change = self.all_data[['trade_dt', 's_info_windcode', 'change']].copy()
        # 数据展开
        self.price_pivot = self.price.pivot('trade_dt', 's_info_windcode', 's_dq_close_today')
        self.change_pivot = self.change.pivot('trade_dt', 's_info_windcode', 'change')
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def roll_t(self, data, index, perid):
        temp = data.copy()
        num = len(data)
        if num > perid:
            temp_mean = temp[index].rolling(perid).mean()
            temp_std = temp[index].rolling(perid).mean()
            t = (temp[index]-temp_mean)/temp_std
            result = pd.DataFrame({'trade_dt': temp.trade_dt.values, 'spreadbias': t.values})
            return result
        else:
            result = pd.DataFrame({'trade_dt': temp.trade_dt.values, 'pricedelay': [None for i in range(num)]})
            return result

    def compute(self):
        t = time.time()
        # 逐日计算每个股票的参考价格
        num = len(self.change_pivot.index)
        self.ref_price = []
        for i in range(250, num):
            # 当前交易日
            date = self.change_pivot.index[i - 1]
            print(date)
            # 提取前250个交易日股价、涨幅数据(剔除nan)，并对齐
            temp_price = self.price_pivot.iloc[i - 250:i, :].dropna(axis=1, how='any')
            temp_change = self.change_pivot.iloc[i - 250:i, :].dropna(axis=1, how='any')
            temp_price = temp_price.reindex(columns=temp_change.columns)
            # 根据涨幅数据计算股票相关系数矩阵，并将其转化为距离矩阵
            temp_dist = pd.DataFrame(1 - np.corrcoef(temp_change.T))
            # 取距离矩阵每列1%分位的距离值
            temp_quantiles = temp_dist.quantile(q=0.01, axis=0)
            # 取每个股票距离其最近的1%股票（矩阵中被标为1）
            temp_nearst = ((temp_dist.values - temp_quantiles.values) <= 0) + 0
            # 取当期股价数据
            temp_price_now = temp_price.iloc[-1, :]
            # 计算股票参考价格
            temp_mean = (np.dot(temp_price_now.values, temp_nearst)-temp_price_now.values)/(temp_nearst.sum(axis=0)-1)
            # 转Dataframe
            temp_result = pd.DataFrame(temp_mean, index=temp_price_now.index, columns={date}).T
            # 补充之前剔除的股票（自动填充为NaN）
            result = temp_result.reindex(columns=self.change_pivot.columns)
            # 数据存放
            self.ref_price.append(result)
        # 参考价格数据合并
        self.ref_price = pd.concat(self.ref_price)
        # 参考价格数据压缩
        self.ref_price_new = self.ref_price.unstack() \
                                           .reset_index() \
                                           .rename(columns={'level_1': 'trade_dt', 0: 'ref_price'})
        # merge参考价格、股价
        self.data_sum = pd.merge(self.ref_price_new, self.price, how='left')
        # 计算对数价差
        self.data_sum['pricespread'] = np.log(self.data_sum['s_dq_close_today']) - np.log(self.data_sum['ref_price'])
        # 计算价差偏离度
        self.temp_result = self.data_sum.groupby('s_info_windcode')\
                                        .apply(self.roll_t, 'pricespread', 60)\
                                        .reset_index()
        print('compute running time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.all_data = pd.read_pickle(self.file_indir + 'all_dayindex.pkl')
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'spreadbias']
        self.result[item].to_pickle(self.save_indir + 'factor_price_spreadbias.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('end running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_dayindex.pkl'
    
    sb = Spreadbias(file_indir, save_indir, file_name)
    sb.runflow()
