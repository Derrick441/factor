#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import sqlconn
# from decorators import decorators_runtime


class DataflowIndugen(object):

    def __init__(self, index, indir, enddate):
        self.index = index
        self.indir = indir
        self.enddate = enddate

    def filein(self):
        # self.dates = pd.read_csv(self.indir+self.index+'\\'+self.index+'_dates.csv',dtype=str,sep=',',header=None)
        self.band_date_stock = pd.read_pickle(self.indir+self.index+'/'+self.index+'_band_dates_stocks_closep.pkl')

    # @decorators_runtime
    def sqlin(self):
        conn = sqlconn.sqlconn()
        # 个股行业变动表
        sqlquery = 'select S_INFO_WINDCODE "s_info_windcode", CITICS_IND_CODE "inducode", ENTRY_DT "trade_dt",' + \
                   'REMOVE_DT "remove_dt" from wind.AShareIndustriesClassCITICS ' + \
                   'where ENTRY_DT<='+self.enddate+' order by ENTRY_DT,S_INFO_WINDCODE'
        self.data = pd.read_sql(sqlquery, conn)
        # 各级行业代码
        sqlquery = 'select IndustriesCode "inducode",Industriesname "induname",levelnum "levelnum" ' + \
                   'from wind.AShareIndustriesCode ' + \
                   'where levelnum>1 and levelnum<5 order by IndustriesCode'
        self.induname = pd.read_sql(sqlquery, conn)
        conn.close()

    def indudata_changename_sub(self, colname, lnum):
        # 将行业代码转换成行业名称 子函数
        induname0 = self.induname[self.induname.levelnum == lnum].copy()
        induname0['inducode1'] = induname0['inducode'].apply(lambda x: x[0:2*lnum])
        induname0.set_index('inducode1', inplace=True)
        self.data[colname] = self.data['inducode'].apply(lambda x: induname0['induname'].to_dict().get(x[0:2*lnum]))

    # @decorators_runtime
    def indudata_changename(self):
        # 将行业代码转换成行业名称
        self.indudata_changename_sub('induname1', 2)
        self.indudata_changename_sub('induname2', 3)
        self.indudata_changename_sub('induname3', 4)
        pass

    # @decorators_runtime
    def indudata_merge(self):
        # self.data['trade_dt']=self.data['trade_dt'].apply(int)
        factors = ['induname1', 'induname2', 'induname3']
        self.mergedata = pd.merge(self.band_date_stock, self.data, how='outer', on=['trade_dt', 's_info_windcode'])
        self.mergedata.sort_values(by=['s_info_windcode', 'trade_dt'], inplace=True)  # group by will keep the order
        grouped = self.mergedata.groupby(self.mergedata['s_info_windcode'])
        self.mergedata[factors] = grouped[factors].ffill()[factors]
        self.mergedata.drop(columns='remove_dt', inplace=True)
        self.mergedata.dropna(subset=['s_dq_close'], inplace=True)

    def indudata_mix(self):
        # 将部分二级行业与一级行业共同使用
        idx1 = self.mergedata['induname1'].isin(['银行', '非银行金融', '电子', '建筑', '交通运输', '国防军工', '农林牧渔',
                                                 '汽车', '轻工制造', '食品饮料', '通信', '银行', '医药', '有色'])
        self.mergedata.loc[idx1, 'indumix1'] = self.mergedata.loc[idx1, 'induname2']

        idx2 = self.mergedata['induname1'].isin(['房地产'])
        self.mergedata.loc[idx2, 'indumix1'] = self.mergedata.loc[idx2, 'induname3']

        idx3 = self.mergedata['induname3'].isin(['白酒', '水泥'])
        self.mergedata.loc[idx3, 'indumix1'] = self.mergedata.loc[idx3, 'induname3']

        idx = ~(idx1 | idx2 | idx3)
        self.mergedata.loc[idx, 'indumix1'] = self.mergedata.loc[idx, 'induname1']

    def fileout(self):
        self.data.to_pickle(self.indir+self.index+'\\'+self.index+'_indu.pkl')
        self.mergedata.to_pickle(self.indir+self.index+'\\'+self.index+'_band_indu.pkl')

    def runflow(self):
        self.filein()
        self.sqlin()
        self.indudata_changename()
        self.indudata_merge()
        self.indudata_mix()
        self.fileout()


if __name__ == '__main__':
    file_index = 'all'
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    data_enddate = '20200630'
    indugen = DataflowIndugen(file_index, file_indir, data_enddate)
    indugen.runflow()
