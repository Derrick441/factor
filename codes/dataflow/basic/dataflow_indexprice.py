#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from sqlconn import sqlconn
class dataflow_indexprice(object):
    def __init__(self,INDEX,indir,startdate,enddate):
        self.INDEX = INDEX
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    def sqlin(self):
        conn = sqlconn()
        if self.INDEX=='hs300':
            indexcode = '000300.SH'
        elif self.INDEX=='zz500':
            indexcode = '000905.SH'
        elif self.INDEX=='zz800':
            indexcode = '000906.SH'
        elif self.INDEX=='sz50':
            indexcode = '000016.SH'
        else:
            indexcode = self.INDEX

        # sqlq = "select s_info_windcode,report_period,ann_dt as trade_dt,"+factorstr+" from wind."+self.ftable+"\
        # where statement_type='408001000' and report_period>'"+self.longtimeago+"' and \
        # s_info_windcode<='699999.SH' and ann_dt<='"+self.enddate+"' order by report_period"
        sqlq = "select trade_dt,s_info_windcode,s_dq_preclose,s_dq_close,s_dq_change from wind.Aindexeodprices "+ \
                "where S_INFO_WINDCODE = '"+indexcode+"' and trade_dt>='"+self.startdate+"' and trade_dt<='"+self.enddate+"' order by trade_dt"
        self.data = pd.read_sql(sqlq,conn)
        self.data.rename(columns=lambda x:x.lower(),inplace=True)
        self.data.set_index(['trade_dt'],inplace=True)
        conn.close()

    def fileout(self):
        self.data.to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_indexprice.pkl')

    def run_flow(self):
        self.sqlin()
        self.fileout()

if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    startdate = '20050408'
    enddate = '20200801'
    # INDEX = 'hs300'
    # indexprice = dataflow_indexprice(INDEX,indir,startdate,enddate)
    # indexprice.run_flow()
    INDEX = 'zz500'
    indexprice = dataflow_indexprice(INDEX,indir,startdate,enddate)
    indexprice.run_flow()
    # INDEX = 'zz800'
    # indexprice = dataflow_indexprice(INDEX,indir,startdate,enddate)
    # indexprice.run_flow()
