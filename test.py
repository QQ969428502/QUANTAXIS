#import sys
#print(sys.path) 
#sys.path.append('e:\\github\\my-QUANTAXIS')
# import enum
# print(enum.__file__) 
import datetime
# import pymongo
#import QUANTAXIS as QA


# stocks=['603709',
#  '603922','603683','603992','603860', '603059', '603895', '603332', '603657', '002931', '603192', '603500', '603499', '603687', '002919', '603629',
#         '603076', '603676', '002910', '603617', '603136', '002871', '603829', '002830', '603386','002896','603813', '002888', '603110', '002875', 
#         '002882', '002917', '002898', '601086', '603790']
# current_date='2020-07-10'
# go_time=current_date+' 14:50:00' 

# data_min_tushare=QA.QAFetch.QATushare.QA_fetch_get_stock_day('00000.SZ',current_date, go_time)

from QUANTAXIS.QASU.main import(
    QA_SU_save_fundamentals,
    QA_SU_save_securities,
    QA_SU_save_index_min,
    QA_SU_save_stock_day,
    QA_SU_save_index_day,
    QA_SU_save_stock_xdxr,QA_SU_save_stock_list,QA_SU_save_stock_info,QA_SU_save_index_min
    ) 
#from QUANTAXIS.QAUtil.QASetting import (QASETTING)

# QASETTING.set_config('JQUSER','Name','13176651156')
# QASETTING.set_config('JQUSER','Password','Peifeng0219')
# print(QASETTING.get_config('JQUSER','Name'))
# print(QASETTING.get_config('JQUSER','Password'))

if __name__ == '__main__':
   
    #QA_SU_save_fundamentals('jq')
    #QA_SU_save_securities('jq')
    #QA_SU_save_index_min('jq') 
    #QA_SU_save_stock_list('tdx')
    #QA_SU_save_stock_info('tdx')
    #QA_SU_save_stock_day('tdx')
    #QA_SU_save_index_day('tdx')
    #QA_SU_save_stock_xdxr('tdx')
    pass
     