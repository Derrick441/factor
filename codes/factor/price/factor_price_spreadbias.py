import pandas as pd
import numpy as np
import time
np.seterr(invalid='ignore')

# 日频率
# 价差偏离度：股价与参考价格的对数价差的偏离度
# 参考价格：根据个股与所有股票的相似度（250交易日涨跌幅相似度），选取1%的股票作为参考组合，取组合平均价作为参考价格
# ln_price_spread=ln(p)-ln(ref_p)   [ln_price_spread-mean(ln_price_spread,60)]/std(ln_price_spread,60)


class Spreadbias(object):

    def __init__(self, indir, index):
        self.indir = indir
        self.index = index

    def filein(self):
        t = time.time()
        self.price = pd.read_pickle(self.indir + self.index + '/' + self.index + '_band_dates_stocks_closep.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def data_manage(self):
        t = time.time()
        self.price_pivot = self.price.pivot('trade_dt', 's_info_windcode', 's_dq_close')
        print('pivot running time:%10.4fs' % (time.time()-t))

        t = time.time()
        self.ref_price_pivot = []
        for i in range(250, self.price_pivot.shape[0]):
            print(self.price_pivot.index[i-1])
            # 提取250个交易日股价数据（剔除有nan的股票）
            temp_pivot_na = self.price_pivot.iloc[i - 250:i, :].dropna(axis=1, how='any')
            # 股价数据转涨幅数据(剔除nan)
            temp_change_na = (temp_pivot_na.pct_change() * 100).dropna(axis=0, how='any')
            # 计算相关系数矩阵，并将其转化为距离矩阵
            temp_dist = pd.DataFrame(1 - np.corrcoef(temp_change_na.T))

            # 取距离矩阵每列1%分位的距离值
            temp_1_quantile_dist = temp_dist.quantile(q=0.01, axis=0, interpolation='higher')
            # 标记距离矩阵每列距离值小于1%分位值的股票
            temp_nearst = ((temp_dist.values - temp_1_quantile_dist.values) <= 0) + 0
            # 取当期股价数据
            temp_pivot_now = temp_pivot_na.iloc[-1, :]
            # 计算股票参考价格（相似股票价格合计-自身股票价格，除相似股票数（距离最近的1%股票数-本身），得到平均价格，即参考价格）
            temp_mean = (np.dot(temp_pivot_now.values, temp_nearst)-temp_pivot_now.values) / (temp_nearst.sum(axis=0)-1)

            # np.array格式转dataframe格式，并补充之前剔除的股票（自动填充为NaN）
            temp_result = pd.DataFrame(temp_mean, index=temp_pivot_now.index, columns=[self.price_pivot.index[i - 1]])\
                .T.reindex(columns=self.price_pivot.columns)
            # 存放
            self.ref_price_pivot.append(temp_result)
        print('ref_price running time:%10.4fs' % (time.time()-t))

        # 合并全部日期的参考价格数据
        t = time.time()
        self.ref_price = pd.concat(self.ref_price_pivot)
        # 数据格式调整
        self.ref_price_new = self.ref_price.unstack().reset_index()\
                                           .rename(columns={'level_1': 'trade_dt', 0: 'ref_price'})
        print('form_adjust running time:%10.4fs' % (time.time()-t))

        # 合并参考价格、股价数据
        t = time.time()
        self.data_sum = pd.merge(self.ref_price_new, self.price, how='left')
        print('merge running time:%10.4fs' % (time.time()-t))

    def compute_spreadbias(self):
        t = time.time()

        # 计算对数价差
        self.data_sum['pricespread'] = np.log(self.data_sum['s_dq_close']) - np.log(self.data_sum['ref_price'])
        # 计算每个股票对数价差的60日均值和60日标准差
        self.data_sum['pricespread_60_mean'] = self.data_sum.groupby(['s_info_windcode'])['pricespread']\
                                                            .rolling(60).mean().values
        self.data_sum['pricespread_60_std'] = self.data_sum.groupby(['s_info_windcode'])['pricespread']\
                                                           .rolling(60).std().values
        # 计算价差偏离度
        temp = self.data_sum['pricespread'] - self.data_sum['pricespread_60_mean']
        self.data_sum['spreadbias'] = temp / self.data_sum['pricespread_60_std']

        # 去除nan值
        self.data_sum.dropna(inplace=True)
        print('compute_spreadbias running time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        item = ['trade_dt', 's_info_windcode', 'spreadbias']
        self.data_sum[item].to_pickle(self.indir + 'factor' + '/factor_price_' + self.index + '_spreadbias.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('compute start')
        self.filein()
        self.data_manage()
        self.compute_spreadbias()
        self.fileout()
        print('compute finish, all running time:%10.4fs' % (time.time() - t))
        return self


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    file_index = 'all'
    spreadbias = Spreadbias(file_indir, file_index)
    spreadbias.runflow()

spreadbias.filein()
spreadbias.data_manage()
spreadbias.compute_spreadbias()
