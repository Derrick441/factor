import pandas as pd
from sqlconn import sqlconn


class DataflowIndexprice(object):

    def __init__(self, index, indir, startdate, enddate):
        self.index = index
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    def sqlin(self):
        conn = sqlconn()
        if self.index == 'hs300':
            indexcode = '000300.SH'
        elif self.index == 'zz500':
            indexcode = '000905.SH'
        elif self.index == 'zz800':
            indexcode = '000906.SH'
        elif self.index == 'sz50':
            indexcode = '000016.SH'
        else:
            indexcode = self.index

        # sqlq = "select s_info_windcode,report_period,ann_dt as trade_dt,"+factorstr+" from wind."+self.ftable+"\
        # where statement_type='408001000' and report_period>'"+self.longtimeago+"' and \
        # s_info_windcode<='699999.SH' and ann_dt<='"+self.enddate+"' order by report_period"

        sqlq = "select trade_dt,s_info_windcode,s_dq_preclose,s_dq_close,s_dq_change from wind.Aindexeodprices " + \
               "where S_INFO_WINDCODE = '"+indexcode+"' " \
               "and trade_dt>='"+self.startdate+"' " \
               "and trade_dt<='"+self.enddate+"' " \
               "order by trade_dt"

        self.data = pd.read_sql(sqlq, conn)
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        self.data.set_index(['trade_dt'], inplace=True)
        conn.close()

    def fileout(self):
        self.data.to_pickle(self.indir+self.index+'/'+self.index+'_indexprice.pkl')

    def runflow(self):
        self.sqlin()
        self.fileout()
        print('end')


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    data_startdate = '20050101'
    data_enddate = '20191231'
    file_index = 'zz500'
    indexprice = DataflowIndexprice(file_index, file_indir, data_startdate, data_enddate)
    indexprice.runflow()

    # index = 'hs300'
    # indexprice = DataflowIndexprice(index,indir,startdate,enddate)
    # indexprice.runflow()
    # index = 'zz800'
    # indexprice = DataflowIndexprice(index,indir,startdate,enddate)
    # indexprice.runflow()
