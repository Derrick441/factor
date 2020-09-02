import time
import pandas as pd
import sqlconn
from sqlconn import sqlGetIp


class DataflowFreeweight(object):

    def __init__(self, index, indir, startdate, enddate):
        self.index = index
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    def sqlin(self):

        if self.index == 'sz50':
            indexcode = '46'
        elif self.index == 'zz500':
            indexcode = '4978'
        else:
            indexcode = '3145'
        conn = sqlconn.sqlconnJYDB()
        starttime = time.time()
        # 取 次日 权重和调整市值
        if sqlGetIp() == '10.17.12.8':
            sqlquery = "select A.indexCODE, A.ENDDATE as trade_dt, A.ADJUSTEDMV, A.WEIGHTEDRATIO, " \
                       "B.Secucode as s_info_windcode, A.DataType " \
                       "from JYHQ.Sa_Tradableshare A join JYHQ.Secumain B on A.Innercode = B.Innercode " \
                       "where A.ENDDATE >= TO_DATE('" + self.startdate + "', 'yyyy/MM/DD HH24:MI:SS') " \
                       "and A.ENDDATE <= TO_DATE('" + self.enddate + "', 'yyyy/MM/DD HH24:MI:SS') " \
                       "and A.indexCODE = " + indexcode + " order by A.ENDDATE"
        else:
            sqlquery = "select A.indexCODE, A.ENDDATE as trade_dt, A.ADJUSTEDMV, A.WEIGHTEDRATIO, " \
                       "B.Secucode as s_info_windcode, A.DataType " \
                       "from JYDB.Sa_Tradableshare A join JYDB.Secumain B on A.Innercode = B.Innercode " \
                       "where A.ENDDATE >= TO_DATE('" + self.startdate + "', 'yyyy/MM/DD HH24:MI:SS') " \
                       "and A.ENDDATE <= TO_DATE('" + self.enddate + "', 'yyyy/MM/DD HH24:MI:SS') " \
                       "and A.indexCODE = " + indexcode + " order by A.ENDDATE"
            # " and A.DATATYPE = 2 order by A.ENDDATE"
        self.data = pd.read_sql(sqlquery, conn)
        endtime = time.time()
        print('sql running time:%10.4fs' % (endtime-starttime))

    def stocks_str_format(self, secucode):
        stocknum = int(secucode)
        return ('%06d' % stocknum)+'.SH' if stocknum >= 600000 else ('%06d' % stocknum)+'.SZ'

    def datamanage(self):
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        self.data['trade_dt'] = self.data['trade_dt'].apply(lambda x: x.strftime('%Y%m%d'))
        self.data['s_info_windcode'] = self.data['s_info_windcode'].apply(lambda x: self.stocks_str_format(x))
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.wcur = self.data[self.data['datatype'] == 1].copy()
        self.wnext = self.data[self.data['datatype'] == 2].copy()
        self.wcur.drop(['indexcode', 'datatype'], axis=1, inplace=True)
        self.wnext.drop(['indexcode', 'datatype'], axis=1, inplace=True)

    def fileout(self):
        self.wcur.to_pickle(self.indir+self.index+'/'+self.index+'_freeweight_curdate.pkl')
        self.wnext.to_pickle(self.indir+self.index+'/'+self.index+'_freeweight.pkl')

    def run_flow(self):
        self.sqlin()
        self.datamanage()
        self.fileout()
        print('end')


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    data_startdate = '20050101'
    data_enddate = '20191231'
    # freew.index = 'hs300'
    # freew.run_flow()
    file_index = 'zz500'
    freew = DataflowFreeweight(file_index, file_indir, data_startdate, data_enddate)
    freew.run_flow()
