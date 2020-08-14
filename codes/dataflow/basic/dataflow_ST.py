import pandas as pd
import numpy as np
import sqlconn
# from decorators import decorators_runtime

class dataflow_ST(object):

    def __init__(self, indir, INDEX, enddate):
        self.indir = indir
        self.INDEX = INDEX
        self.enddate = enddate

    def filein(self):
        self.band_date_stock = pd.read_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_band_dates_stocks_closep.pkl')
        self.band_date_stock.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.stflag = pd.DataFrame(index=self.band_date_stock.index)
        self.stflag['stflag'] = 1

    # @decorators_runtime
    def sqlin(self):
        conn = sqlconn.sqlconn()
        # 个股行业变动表
        sqlquery = 'select s_info_windcode,s_type_st,entry_dt,remove_dt from wind.AshareST'
        self.data = pd.read_sql(sqlquery, conn)
        self.data.rename(columns=lambda x:x.lower(), inplace=True)
        conn.close()

    def sttype2num(self, item):
        if item == 'S':
            return 1
        elif item == 'P':
            return 2
        elif item == 'L':
            return 3
        elif item == 'Z':
            return 4
        else:
            return 5

    def ST_gen(self):
        idxs = pd.IndexSlice
        self.stflag['ST'] = 0
        self.stflag.sort_index(inplace=True)
        for index,row in self.data.iterrows():
            # 只处理在 stflag 内的个股
            print(row['s_info_windcode'])
            if self.stflag.index.isin([row['s_info_windcode']],level=1).any():
                if isinstance(row['remove_dt'], str):
                    self.stflag.loc[idxs[row['entry_dt']:row['remove_dt'],row['s_info_windcode']], 'ST'] = self.sttype2num(row['s_type_st'])
                else:
                    self.stflag.loc[idxs[row['entry_dt']:, row['s_info_windcode']], 'ST'] = self.sttype2num(row['s_type_st'])
        self.stflag['ST'].to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_ST.pkl')
        pass

    def run_flow(self):
        self.filein()
        self.sqlin()
        self.ST_gen()

if __name__=='__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    enddate = '20200630'
    ST = dataflow_ST(indir, INDEX, enddate)
    ST.run_flow()
