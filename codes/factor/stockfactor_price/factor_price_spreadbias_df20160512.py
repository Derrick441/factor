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
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.price = self.all_data[['trade_dt', 's_info_windcode', 's_dq_close']].copy()
        self.change = self.all_data[['trade_dt', 's_info_windcode', 's_dq_pctchange']].copy()
        self.price_pivot = self.price.pivot('trade_dt', 's_info_windcode', 's_dq_close')
        self.change_pivot = self.change.pivot('trade_dt', 's_info_windcode', 's_dq_pctchange')
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def ref_p(self, price, change, q):
        temp_price = price.copy()
        temp_change = change.copy()
        # 对齐
        temp_price = temp_price.reindex(columns=temp_change.columns)
        # 股票相关系数矩阵
        corr = np.corrcoef(temp_change.T)
        # 股票距离矩阵
        distance = pd.DataFrame(1 - corr)
        # 取距离矩阵每列q分位的距离值
        q_values = distance.quantile(q=q, axis=0)
        # 取距离值小于q_values的股票
        near = ((distance.values - q_values.values) <= 0) + 0
        # 取当期股价
        price_now = temp_price.iloc[-1, :]
        # 计算股票参考价格
        refprice = (np.dot(price_now.values, near) - price_now.values) / (near.sum(axis=0) - 1)
        # 转Dataframe
        result = pd.DataFrame(refprice, index=price_now.index, columns={price_now.name}).T
        return result

    def roll_t(self, data, index, perid):
        temp = data.copy()
        num = len(data)
        if num > perid:
            temp_mea = temp[index].rolling(perid).mean()
            temp_std = temp[index].rolling(perid).std()
            t = (temp[index]-temp_mea)/temp_std
            result = pd.DataFrame({'trade_dt': temp.trade_dt.values, 'spreadbias': t.values})
            return result
        else:
            result = pd.DataFrame({'trade_dt': temp.trade_dt.values, 'spreadbias': [None for i in range(num)]})
            return result

    def compute(self):
        t = time.time()
        dates = self.change_pivot.index
        dates_num = len(dates)
        self.refprice = []
        # 逐日计算每股参考价格
        for i in range(250, dates_num):
            # 当前交易日
            print(dates[i - 1])
            # 取250日股价、涨幅数据
            temp_price = self.price_pivot.iloc[i - 250:i, :].dropna(axis=1, how='any')
            temp_change = self.change_pivot.iloc[i - 250:i, :].dropna(axis=1, how='any')
            # 计算参考价格
            temp_ref = self.ref_p(temp_price, temp_change, 0.01)
            # 填充被剔除股票（自动填充为NaN）
            temp_ref_fill = temp_ref.reindex(columns=self.change_pivot.columns)
            # 数据存放
            self.refprice.append(temp_ref_fill)
        # 参考价格数据合并
        self.refprice = pd.concat(self.refprice)
        # 数据unstack
        self.refprice_unstack = self.refprice.unstack() \
                                             .reset_index() \
                                             .rename(columns={'level_1': 'trade_dt', 0: 'refprice'})
        # merge参考价格、股价
        self.data_sum = pd.merge(self.refprice_unstack, self.price, how='left')
        # 计算对数价差
        self.data_sum['pricespread'] = np.log(self.data_sum['s_dq_close']) - np.log(self.data_sum['refprice'])
        # 计算价差偏离度
        self.temp_result = self.data_sum.groupby('s_info_windcode')\
                                        .apply(self.roll_t, 'pricespread', 60)\
                                        .reset_index()
        print('compute running time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
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
        print('finish running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_dayindex.pkl'
    
    sb = Spreadbias(file_indir, save_indir, file_name)
    sb.runflow()
