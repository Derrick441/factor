from ai_strategy import AiStrategy

dayN = 5
# indir = '../../../data/developflow/'
# outdir = '../../../data/result/'
indir = 'D:\\lirx\\data\\developflow\\'
outdir = 'D:\\lirx\\data\\result\\'
INDEX = 'zz500'
hedgeindex = 'zz500'
rollN = 5
scale = 0.1
startdate = '20170103'
enddate = '20191231'
costsell = 0.0025
costbuy = 0.0015
trade_open = 'vwap_hh'
topgroup = 1
debug = False
# factorname = 'feature179T_mbase179_mshift179_mshiftbase7_hp-5-0.395-0.056-43-' \
#              '849-1079_listdays_l1_rand2_adj'+trade_open
# factorname = 'feature179T_mbase179_mshift179_mshiftbase7_hp-5-0.395-0.056-43-849-1079_' \
#              '63_listdays_l1_rand2_adjvwap_hh'

# factorname = 'feature179T_mbaseM179_mshift179_mshiftbaseM7_hp-5-0.395-0.056-43-849-1079_' \
#              '63_listdays_l1_rand2_adjvwap_hh'

factorname = 'feature179TAB_base179B_shift179_shiftbase7_hp-5-0.103-0.06128-12-571-990_63_st12_l1_rand2_adjvwap_hh'
# factorname = 'feature179TAB_base179B_shift179_shiftbase7_hp-5-0.395-0.056-43-849-1079_63_st12_l1_rand2_adjvwap_hh'

strategy = AiStrategy(INDEX,indir,hedgeindex,outdir,startdate,enddate,factorname,scale,
                      rollN,dayN,costsell,costbuy,trade_open,topgroup,debug)
# strategy.debug = True
strategy.run_flow()
