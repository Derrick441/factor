import pandas as pd
from sqlconn import sqlconnJYDB


class HqflowCompleteStoredata(object):

    def __init__(self, file_indir, file_name, startdate, enddate, hqtype):
        self.file_indir = file_indir
        self.file_name = file_name
        self.startdate = startdate
        self.enddate = enddate
        self.hqtype = hqtype
        self.hqdata = pd.DataFrame()

    def filein(self):
        self.dates = pd.read_pickle(self.file_indir + 'all_dates.pkl')
        self.dates.drop_duplicates(inplace=True)
        self.dates = self.dates.loc[(self.dates >= self.startdate) & (self.dates <= self.enddate)]
        self.dates = self.dates.to_frame('trade_dt')

    def hqflowjydbsql(self, curday):
        conn = sqlconnJYDB()
        sqlq = 'select stockcode,type,bargaindate,bargaintime,openprice,highprice,lowprice,closeprice,volume,turover ' \
               'from JYHQ.M_'+curday + \
               ' where type=\''+self.hqtype+'\' and stockcode=\'000905.SH\'' \
               'order by stockcode,bargaindate,bargaintime'
        try:
            sqldata = pd.read_sql(sqlq, conn)
            sqldata.rename(columns=lambda x: x.lower(), inplace=True)
            temp = {'stockcode': 's_info_windcode', 'bargaindate': 'trade_dt', 'turover': 'amount'}
            sqldata.rename(columns=temp, inplace=True)
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
                indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\'
                yearhq.to_pickle(indir + 'zz500_store_hqdata_'+str(year)+'.pkl')

    def runflow(self):
        self.filein()
        self.hqdatayearfetch()


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name = 'all_band_dates_stocks_closep.pkl'
    startdate = '20050101'
    enddate = '20191231'
    hcs = HqflowCompleteStoredata(file_indir, file_name, startdate, enddate, 'M1')
    hcs.runflow()
