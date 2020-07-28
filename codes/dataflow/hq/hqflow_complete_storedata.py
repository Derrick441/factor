#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import time
from sqlconn import sqlconnJYDB
from reduce_mem_usage_df import reduce_mem_usage
from decorators import decorators_runtime

class HqflowCompleteStoredata(object):

    def __init__(self,indir,INDEX,startdate,enddate,hqtype):
        self.indir = indir
        self.INDEX = INDEX
        self.startdate = startdate
        self.enddate = enddate
        self.hqtype = hqtype
        self.hqdata = pd.DataFrame()

    def fileIn(self):
        self.dates = pd.read_pickle(self.indir+self.INDEX+'\\'+self.INDEX+'_dates.pkl')
        self.dates.drop_duplicates(inplace=True)
        self.dates = self.dates.loc[(self.dates>=self.startdate) &
                                    (self.dates<=self.enddate)]
        self.dates = self.dates.to_frame('trade_dt')

    def hqflowJYDBSql(self,curday):
        # -------------------- Oracel Data Fetch Part -------------------
        # print('getting sql data ...')
        conn = sqlconnJYDB()
        sqlq = 'select stockcode,type,bargaindate,bargaintime,openprice,highprice,lowprice,closeprice,volume,turover ' \
               'from JYHQNEW.M_'+curday+ \
               ' where type=\''+self.hqtype+'\' and (' \
               '((substr(stockcode,1,2) in (\'00\',\'30\')) and substr(stockcode,8,2)=\'SZ\') or ' \
               '((substr(stockcode,1,2) in (\'60\',\'68\')) and substr(stockcode,8,2)=\'SH\') ) ' \
               'order by stockcode,bargaindate,bargaintime'

        try:
            sqldata = pd.read_sql(sqlq,conn)

            sqldata.rename(columns=lambda x:x.lower(),inplace=True)
            sqldata.rename(columns={'stockcode':'s_info_windcode','bargaindate':'trade_dt',
                                    'turover':'amount'},inplace=True)
            sqldata['bargaintime'] = ('0'+sqldata['bargaintime']).str[-6:]
        except:
            sqldata = pd.DataFrame()
        conn.close()
        return sqldata

    def hqDataYearFetch(self):
        self.dates['year'] = self.dates['trade_dt'].str[0:4]
        yearlist = list(self.dates['year'].unique())

        for year in yearlist:
            yearhq = pd.DataFrame()
            print('running hqDataDateFetch year: '+year)
            curyeardates =self.dates.loc[self.dates['year']==year]

            for item in list(curyeardates['trade_dt'].unique()):
                datehq = self.hqflowJYDBSql(item)
                if not datehq.empty:
                    print('running hqDataDateFetch : '+item)
                    yearhq = pd.concat([yearhq,datehq],axis=0)

            if not yearhq.empty:
                yearhq.to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_store_hqdata_'+str(year)+'.pkl')

    def runFlow(self):
        self.fileIn()
        self.hqDataYearFetch()

if __name__=='__main__':

    indir = '../../../data/investflow/'
    INDEX = 'all'
    startdate = '20120102'
    enddate = '20200723'
    hcs = HqflowCompleteStoredata(indir,INDEX,startdate,enddate,'M1')
    hcs.runFlow()



