import pandas as pd
import numpy as np
import sqlconn
# from decorators import decorators_runtime

class dataflow_listdate(object):

    def __init__(self, INDEX, indir):
        self.INDEX = INDEX
        self.indir = indir

    def filein(self):
        self.band_date_stock = pd.read_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_band_dates_stocks_closep.pkl')
        self.band_date_stock.set_index(['trade_dt','s_info_windcode'],inplace=True)
        self.stflag = pd.DataFrame(index=self.band_date_stock.index)
        self.stflag['stflag'] = 1

    # @decorators_runtime
    def sqlin(self):
        conn = sqlconn.sqlconn()
        # 上市表
        sqlquery = 'select s_info_windcode,s_info_listdate,s_info_listboardname from wind.AshareDescription '+ \
                   'where substr(s_info_windcode,1,1) in (\'0\',\'3\',\'6\')'
        self.data = pd.read_sql(sqlquery,conn)
        self.data.rename(columns=lambda x:x.lower(),inplace=True)
        conn.close()

    def boardname2num(self,item):
        if item == '主板':
            return 0
        elif item == '中小企业板':
            return 1
        elif item == '创业板':
            return 2
        elif item == '科创板':
            return 3
        else:
            return 0

    def listdate_gen(self):
        self.data = self.data[self.data['s_info_listdate'].isnull()==False]
        self.stflag.reset_index(inplace=True)
        listinfo = self.stflag.merge(self.data,how='left',on='s_info_windcode')
        tradedt = pd.to_datetime(listinfo['trade_dt'],format='%Y%m%d')
        s_info_listdate = pd.to_datetime(listinfo['s_info_listdate'],format='%Y%m%d')
        listinfo['listdays'] = (tradedt-s_info_listdate).dt.days
        listinfo[listinfo['listdays']<0] = 0
        listinfo.set_index(['trade_dt','s_info_windcode'],inplace=True)
        listinfo['listdays'].to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_listdays.pkl')
        pass

    def run_flow(self):
        self.filein()
        self.sqlin()
        self.listdate_gen()

if __name__=='__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    listdate = dataflow_listdate(INDEX, indir)
    listdate.run_flow()
