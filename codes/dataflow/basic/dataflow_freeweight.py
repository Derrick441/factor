#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import time
import pandas as pd
import numpy as np
import sqlconn
import cx_Oracle
import itertools
from decorators import decorators_runtime
from sqlconn import sqlGetIp

class dataflow_freeweight(object):
    # 从聚源数据库取指数权重数据
    indir = '../../data/developflow/'
    INDEX = ''
    startdate = ''
    enddate = ''

    def sqlin(self):
        #-------------------- sql get -------------------------------
        if self.INDEX=='sz50':
            indexcode = '46'
        elif self.INDEX=='zz500':
            indexcode = '4978'
        else:
            indexcode = '3145'
        conn = sqlconn.sqlconnJYDB()
        starttime = time.time()
        # 取 次日 权重和调整市值
        if sqlGetIp()=='10.17.12.8':
            sqlquery = "select A.INDEXCODE,A.ENDDATE as trade_dt,A.ADJUSTEDMV,A.WEIGHTEDRATIO,B.Secucode as s_info_windcode," \
                   "A.DataType from JYHQ.Sa_Tradableshare A join JYHQ.Secumain B on A.Innercode = B.Innercode " \
                   "where A.ENDDATE >= TO_DATE('"+self.startdate+"', 'yyyy/MM/DD HH24:MI:SS') " \
                   "and A.ENDDATE <= TO_DATE('"+self.enddate+"', 'yyyy/MM/DD HH24:MI:SS') and A.INDEXCODE = "+indexcode+\
                   " order by A.ENDDATE"
        else:
            sqlquery = "select A.INDEXCODE,A.ENDDATE as trade_dt,A.ADJUSTEDMV,A.WEIGHTEDRATIO,B.Secucode as s_info_windcode," \
                       "A.DataType from JYDB.Sa_Tradableshare A join JYDB.Secumain B on A.Innercode = B.Innercode " \
                       "where A.ENDDATE >= TO_DATE('" + self.startdate + "', 'yyyy/MM/DD HH24:MI:SS') " \
                                                                         "and A.ENDDATE <= TO_DATE('" + self.enddate + "', 'yyyy/MM/DD HH24:MI:SS') and A.INDEXCODE = " + indexcode + \
                       " order by A.ENDDATE"
            # " and A.DATATYPE = 2 order by A.ENDDATE"
        self.data = pd.read_sql(sqlquery,conn)
        endtime = time.time()
        print('sql running time:%10.4fs' % (endtime-starttime))

    def stocks_str_format(self,secucode):
        stocknum = int(secucode)
        return ('%06d'%stocknum)+'.SH' if stocknum>=600000 else ('%06d'%stocknum)+'.SZ'

    def data_formatchange(self):
        self.data.rename(columns=lambda x:x.lower(),inplace=True)
        self.data['trade_dt']=self.data['trade_dt'].apply(lambda x: x.strftime('%Y%m%d'))
        self.data['s_info_windcode']=self.data['s_info_windcode'].apply(lambda x: self.stocks_str_format(x))
        self.data.set_index(['trade_dt','s_info_windcode'],inplace=True)
        self.wcur = self.data[self.data['datatype']==1].copy()
        self.wnext = self.data[self.data['datatype']==2].copy()
        self.wcur.drop(['indexcode', 'datatype'], axis=1, inplace=True)
        self.wnext.drop(['indexcode', 'datatype'], axis=1, inplace=True)

    def fileout(self):
        self.wcur.to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_freeweight_curdate.pkl')
        self.wnext.to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_freeweight.pkl')

    def run_flow(self):
        self.sqlin()
        self.data_formatchange()
        self.fileout()

if __name__ == '__main__':
    # freew = dataflow_freeweight(INDEX,indir,startdate,enddate)
    freew = dataflow_freeweight()
    freew.indir = '../../data/developflow/'
    freew.startdate = '20050428'
    freew.enddate = '20200106'
    # freew.INDEX = 'hs300'
    # freew.run_flow()
    freew.INDEX = 'zz500'
    freew.run_flow()
