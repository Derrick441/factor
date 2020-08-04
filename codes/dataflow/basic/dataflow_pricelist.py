#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import time
import pandas as pd
import numpy as np
import sqlconn
import itertools
# from decorators import decorators_runtime

class dataflow_pricelist(object):
    def __init__(self,INDEX,indir,startdate,enddate):
        self.INDEX = INDEX
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    # def filein(self):
    #     self.dates = np.loadtxt(self.indir+self.INDEX+'\\'+self.INDEX+'_dates.csv',dtype=str)

    def sqlin(self):
        #-------------------- sql get -------------------------------
        conn = sqlconn.sqlconn()
        starttime = time.time()
        sqlquery = 'select s_info_windcode,trade_dt,s_dq_close,s_dq_open,s_dq_high,s_dq_low,' \
                   's_dq_volume,s_dq_amount,s_dq_adjfactor from wind.AShareEODPrices where trade_dt>='+self.startdate+ \
                   ' and trade_dt<='+self.enddate
        self.data = pd.read_sql(sqlquery,conn)
        self.data.rename(columns=lambda x:x.lower(),inplace=True)
        endtime = time.time()
        print('sql running time:%10.4fs' % (endtime-starttime))

    # @decorators_runtime
    def data_fillna(self):
        factor = ['s_dq_close','s_dq_open','s_dq_high','s_dq_low','s_dq_adjfactor']
        self.data.sort_values(by=['s_info_windcode','trade_dt'],inplace=True)
        grouped = self.data.groupby('s_info_windcode')
        self.data[factor] = grouped[factor].ffill()[factor]
        self.data.loc[:,['s_dq_volume','s_dq_amount']].fillna(0,inplace=True)

    # @decorators_runtime
    def data_fillna_pivot(self):
        factor = ['s_dq_open','s_dq_high','s_dq_low','s_dq_adjfactor']
        self.data.sort_values(by=['s_info_windcode','trade_dt'],inplace=True)
        self.mergedata = self.data_fillna_pivot_sub('s_dq_close')
        for item in factor:
            mergetemp = self.data_fillna_pivot_sub(item)
            self.mergedata[item] = pd.merge(self.mergedata,mergetemp,how='left',on=['trade_dt','s_info_windcode'])[item]

    def data_fillna_pivot_sub(self,fname):
        pivotdata = self.data.pivot(index='trade_dt',columns='s_info_windcode',values=fname)
        pivotdata.ffill(inplace=True)
        pivotstackdata = pivotdata.stack().reset_index()
        pivotstackdata.rename(columns={0:fname},inplace=True)
        pivotstackdata.dropna(subset=[fname],inplace=True)
        return pivotstackdata

        # grouped = self.data.groupby('s_info_windcode')
        # self.data[factor] = grouped[factor].ffill()[factor]
        # self.data.loc[:,['s_dq_volume','s_dq_amount']].fillna(0,inplace=True)

    # @decorators_runtime
    def fileout(self):
        #----------------------- to csv as a band ---------------------
        # self.mergedata.to_pickle(self.indir+self.INDEX+'\\'+self.INDEX+'_band_price.pkl')
        # self.mergedata[['trade_dt','s_info_windcode','s_dq_close']].to_pickle(self.indir+self.INDEX+'\\'+self.INDEX+'_band_dates_stocks_closep.pkl')
        self.data.to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_band_price.pkl')
        self.data[['trade_dt','s_info_windcode','s_dq_close']].to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_band_dates_stocks_closep.pkl')
        self.data['trade_dt'].to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_dates.pkl')

    def runflow(self):
        # self.filein()
        self.sqlin()
        # self.data_fillna_pivot()
        self.data_fillna()
        self.fileout()
        return self

if __name__ == '__main__':
    INDEX = 'all'
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    startdate = '20050408'
    enddate = '20200801'
    pricelist = dataflow_pricelist(INDEX,indir,startdate,enddate)
    pricelist.runflow()
