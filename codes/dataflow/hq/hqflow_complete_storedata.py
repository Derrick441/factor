import pandas as pd
from sqlconn import sqlconnJYDB


class HqflowCompleteStoredata(object):

    def __init__(self, indir, index, startdate, enddate, hqtype):
        self.indir = indir
        self.index = index
        self.startdate = startdate
        self.enddate = enddate
        self.hqtype = hqtype
        self.hqdata = pd.DataFrame()

    def filein(self):
        self.dates = pd.read_pickle(self.indir+self.index+'\\'+self.index+'_dates.pkl')
        self.dates.drop_duplicates(inplace=True)
        self.dates = self.dates.loc[(self.dates >= self.startdate) &
                                    (self.dates <= self.enddate)]
        self.dates = self.dates.to_frame('trade_dt')

    def hqflowjydbsql(self, curday):
        # -------------------- Oracel Data Fetch Part -------------------
        # print('getting sql data ...')
        conn = sqlconnJYDB()
        sqlq = 'select stockcode,type,bargaindate,bargaintime,openprice,highprice,lowprice,closeprice,volume,turover ' \
               'from JYHQ.M_'+curday + \
               ' where type=\''+self.hqtype+'\' and (' \
               '((substr(stockcode,1,2) in (\'00\',\'30\')) and substr(stockcode,8,2)=\'SZ\') or ' \
               '((substr(stockcode,1,2) in (\'60\',\'68\')) and substr(stockcode,8,2)=\'SH\') ) ' \
               'order by stockcode,bargaindate,bargaintime'

        try:
            sqldata = pd.read_sql(sqlq, conn)
            sqldata.rename(columns=lambda x: x.lower(), inplace=True)
            sqldata.rename(columns={'stockcode': 's_info_windcode', 'bargaindate': 'trade_dt',
                                    'turover': 'amount'}, inplace=True)
            sqldata['bargaintime'] = ('0'+sqldata['bargaintime']).str[-6:]
        except:
            sqldata = pd.DataFrame()
        conn.close()
        return sqldata

    def hqdatayearfetch(self):
        self.dates['year'] = self.dates['trade_dt'].str[0:4]
        yearlist = list(self.dates['year'].unique())

        for year in yearlist:
            yearhq = pd.DataFrame()
            print('running hqDataDateFetch year: '+year)
            curyeardates = self.dates.loc[self.dates['year'] == year]

            for item in list(curyeardates['trade_dt'].unique()):
                datehq = self.hqflowjydbsql(item)
                if not datehq.empty:
                    print('running hqDataDateFetch : '+item)
                    yearhq = pd.concat([yearhq, datehq], axis=0)

            if not yearhq.empty:
                yearhq.to_pickle(self.indir+self.index+'/'+self.index+'_store_hqdata_'+str(year)+'.pkl')

    def runflow(self):
        self.filein()
        self.hqdatayearfetch()


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    file_index = 'all'
    data_startdate = '20120102'
    data_enddate = '20200801'
    hcs = HqflowCompleteStoredata(file_indir, file_index, data_startdate, data_enddate, 'M1')
    hcs.runflow()
