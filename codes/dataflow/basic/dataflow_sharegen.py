#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import sqlconn
# from decorators import decorators_runtime
class DataflowShareGen(object):

    def __init__(self,INDEX,indir,startdate,enddate):
        self.INDEX = INDEX
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    def filein(self):
        # self.dates = pd.read_csv(self.indir+self.INDEX+'\\'+self.INDEX+'_dates.csv',dtype=str,sep=',',header=None)
        self.band_date_stock = pd.read_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_band_dates_stocks_closep.pkl')
        self.band_date_stock.set_index(['trade_dt','s_info_windcode'],inplace=True)
        self.band_date_stock.sort_index(inplace=True)
        self.band_date_stock = self.band_date_stock.loc[self.startdate:]
        # self.band_date_stock = self.band_date_stock.loc[self.band_date_stock['trade_dt']>=self.startdate]

    # @decorators_runtime
    def sqlIn(self):
        conn = sqlconn.sqlconn()
        # 中国股本表
        sqlquery = 'select * from wind.AShareCapitalization where is_valid=1 ' \
                   'order by s_info_windcode,change_dt,opdate'
        self.data = pd.read_sql(sqlquery,conn)
        conn.close()

    def dataPreHandle(self):
        self.data.rename(columns=lambda x:x.lower(),inplace=True)
        self.data.rename(columns={'change_dt':'trade_dt'},inplace=True)
        self.data.drop_duplicates(subset=['trade_dt','s_info_windcode'],keep='first',inplace=True)
        self.data.set_index(['trade_dt','s_info_windcode'],inplace=True)
        self.data.drop(['object_id','wind_code','is_valid','opdate','opmode'],axis=1,inplace=True)

    def dataMap(self):
        self.mdata = pd.DataFrame(index=self.band_date_stock.index)
        mpd = self.mdata.join(self.data,how='outer')
        mpd.sort_index(inplace=True)
        mpd = mpd.groupby(level=1).fillna(method='ffill')
        self.mdata[mpd.columns.to_list()] = mpd

    def mvGen(self):
        self.mdata['tot_mv'] = self.mdata['tot_shr'] * self.band_date_stock['s_dq_close']
        self.mdata['float_mv'] = self.mdata['float_shr'] * self.band_date_stock['s_dq_close']
        self.mdata['float_a_mv'] = self.mdata['float_a_shr'] * self.band_date_stock['s_dq_close']

    def outData(self):
        # self.mdata['year'] = self.mdata.index.get_level_values(level=0).str[0:4]
        # for item in self.mdata['year'].unique():
        #     self.mdata[self.mdata['year']==item].drop(['year'],axis=1).to_pickle(
        #         self.outdir+self.INDEX+'/'+self.INDEX+'_'+str(item)+'_ashare_table.pkl')
        self.mdata.to_pickle(
            self.indir+self.INDEX+'/'+self.INDEX+'_ashare_table.pkl')
        self.mdata['tot_shr'].to_pickle(
            self.indir+self.INDEX+'/'+self.INDEX+'_tot_shr.pkl')
        self.mdata['float_shr'].to_pickle(
            self.indir+self.INDEX+'/'+self.INDEX+'_float_shr.pkl')
        self.mdata['float_a_shr'].to_pickle(
            self.indir+self.INDEX+'/'+self.INDEX+'_float_a_shr.pkl')

        self.mdata['tot_mv'].to_pickle(
            self.indir+self.INDEX+'/'+self.INDEX+'_tot_mv.pkl')
        self.mdata['float_mv'].to_pickle(
            self.indir+self.INDEX+'/'+self.INDEX+'_float_mv.pkl')
        self.mdata['float_a_mv'].to_pickle(
            self.indir+self.INDEX+'/'+self.INDEX+'_float_a_mv.pkl')

    def runflow(self):
        self.filein()
        self.sqlIn()
        self.dataPreHandle()
        self.dataMap()
        self.mvGen()
        self.outData()

if __name__=='__main__':
    INDEX = 'all'
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    startdate = '20050701'
    enddate = '20200630'
    indugen = DataflowShareGen(INDEX,indir,startdate,enddate)
    indugen.runflow()
