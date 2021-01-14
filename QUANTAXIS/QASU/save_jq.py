import sys 
sys.path.append('e:\\github\\my-QUANTAXIS')

import concurrent.futures
import datetime
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pandas as pd
import pymongo

import QUANTAXIS as QA
from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_stock_list
from QUANTAXIS.QAUtil import (DATABASE, QA_util_date_stamp,QA_util_get_trade_range,
                              QA_util_get_real_date, QA_util_log_info,QA_util_log_expection,
                              QA_util_time_stamp, QA_util_to_json_from_pandas,
                              trade_date_sse)
from QUANTAXIS.QAUtil.QASetting import (QASETTING)
TRADE_HOUR_END = 17

JQUSERNAME=QASETTING.get_config('JQUSER','Name')
JQUSERPASSWD=QASETTING.get_config('JQUSER','Password')

# 导入聚宽模块且进行登录
try:
    import jqdatasdk
    # 请自行将 JQUSERNAME 和 JQUSERPASSWD 修改为自己的账号密码
except:
    raise ModuleNotFoundError
jqdatasdk.auth(JQUSERNAME, JQUSERPASSWD)
#jqdatasdk.auth('15550022101','022101')
def now_time():
    """
    1. 当前日期如果是交易日且当前时间在 17:00 之前，默认行情取到上个交易日收盘
    2. 当前日期如果是交易日且当前时间在 17:00 之后，默认行情取到当前交易日收盘
    """
    return (str(
        QA_util_get_real_date(
            str(datetime.date.today() - datetime.timedelta(days=1)),
            trade_date_sse,
            -1,
        )) + " 17:00:00" if datetime.datetime.now().hour < TRADE_HOUR_END else str(
            QA_util_get_real_date(
                str(datetime.date.today()), trade_date_sse, -1)) + " 17:00:00")


def QA_SU_save_stock_min(client=DATABASE, ui_log=None, ui_progress=None):
    """
    聚宽实现方式
    save current day's stock_min data
    """
    

    # 股票代码格式化
    code_list = list(
        map(
            lambda x: x + ".XSHG" if x[0] == "6" else x + ".XSHE",
            QA_fetch_get_stock_list().code.unique().tolist(),
        ))
    coll = client.stock_min
    coll.create_index([
        ("code", pymongo.ASCENDING),
        ("time_stamp", pymongo.ASCENDING),
        ("date_stamp", pymongo.ASCENDING),
    ])
    err = []

    def __transform_jq_to_qa(df, code, type_):
        """
        处理 jqdata 分钟数据为 qa 格式，并存入数据库
        1. jdatasdk 数据格式:
                          open  close   high    low     volume       money
        2018-12-03 09:31:00  10.59  10.61  10.61  10.59  8339100.0  88377836.0
        2. 与 QUANTAXIS.QAFetch.QATdx.QA_fetch_get_stock_min 获取数据进行匹配，具体处理详见相应源码

                          open  close   high    low           vol        amount    ...
        datetime
        2018-12-03 09:31:00  10.99  10.90  10.99  10.90  2.211700e+06  2.425626e+07 ...
        """

        if df is None or len(df) == 0:
            raise ValueError("没有聚宽数据")

        df = df.reset_index().rename(columns={
            "index": "datetime",
            "volume": "vol",
            "money": "amount"
        })

        df["code"] = code
        df["date"] = df.datetime.map(str).str.slice(0, 10)
        df = df.set_index("datetime", drop=False)
        df["date_stamp"] = df["date"].apply(lambda x: QA_util_date_stamp(x))
        df["time_stamp"] = (
            df["datetime"].map(str).apply(lambda x: QA_util_time_stamp(x)))
        df["type"] = type_

        return df[[
            "open",
            "close",
            "high",
            "low",
            "vol",
            "amount",
            "datetime",
            "code",
            "date",
            "date_stamp",
            "time_stamp",
            "type",
        ]]

    def __saving_work(code, coll):
        QA_util_log_info(
            "##JOB03 Now Saving STOCK_MIN ==== {}".format(code), ui_log=ui_log)
        try:
            for type_ in ["1min", "5min", "15min", "30min", "60min"]:
                col_filter = {"code": str(code)[0:6], "type": type_}
                ref_ = coll.find(col_filter)
                end_time = str(now_time())[0:19]
                if coll.count_documents(col_filter) > 0:
                    start_time = ref_[coll.count_documents(
                        col_filter) - 1]["datetime"]
                    QA_util_log_info(
                        "##JOB03.{} Now Saving {} from {} to {} == {}".format(
                            ["1min",
                             "5min",
                             "15min",
                             "30min",
                             "60min"].index(type_),
                            str(code)[0:6],
                            start_time,
                            end_time,
                            type_,
                        ),
                        ui_log=ui_log,
                    )
                    if start_time != end_time:
                        df = jqdatasdk.get_price(
                            security=code,
                            start_date=start_time,
                            end_date=end_time,
                            frequency=type_.split("min")[0]+"m",
                        )
                        __data = __transform_jq_to_qa(
                            df, code=code[:6], type_=type_)
                        if len(__data) > 1:
                            coll.insert_many(
                                QA_util_to_json_from_pandas(__data)[1::])
                else:
                    start_time = "2015-01-01 09:30:00"
                    QA_util_log_info(
                        "##JOB03.{} Now Saving {} from {} to {} == {}".format(
                            ["1min",
                             "5min",
                             "15min",
                             "30min",
                             "60min"].index(type_),
                            str(code)[0:6],
                            start_time,
                            end_time,
                            type_,
                        ),
                        ui_log=ui_log,
                    )
                    if start_time != end_time:
                        __data == __transform_jq_to_qa(
                            jqdatasdk.get_price(
                                security=code,
                                start_date=start_time,
                                end_date=end_time,
                                frequency=type_.split("min")[0]+"m",
                            ),
                            code=code[:6],
                            type_=type_
                        )
                        if len(__data) > 1:
                            coll.insert_many(
                                QA_util_to_json_from_pandas(__data)[1::])
        except Exception as e:
            QA_util_log_info(e, ui_log=ui_log)
            err.append(code)
            QA_util_log_info(err, ui_log=ui_log)

    # 聚宽之多允许三个线程连接
    executor = ThreadPoolExecutor(max_workers=2)
    res = {
        executor.submit(__saving_work, code_list[i_], coll)
        for i_ in range(len(code_list))
    }
    count = 0
    for i_ in concurrent.futures.as_completed(res):
        QA_util_log_info(
            'The {} of Total {}'.format(count,
                                        len(code_list)),
            ui_log=ui_log
        )

        strProgress = "DOWNLOAD PROGRESS {} ".format(
            str(float(count / len(code_list) * 100))[0:4] + "%")
        intProgress = int(count / len(code_list) * 10000.0)

        QA_util_log_info(
            strProgress,
            ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgress
        )
        count = count + 1
    if len(err) < 1:
        QA_util_log_info("SUCCESS", ui_log=ui_log)
    else:
        QA_util_log_info(" ERROR CODE \n ", ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def _get_securities_to_db(securities_coll,current_date): 
    df=jqdatasdk.get_all_securities(types=['stock'],date=current_date)
    df_db=df
    df_db['date']=current_date
    df_db['code_jd']=df_db.index.values
    df_db['code']=df_db['code_jd'].apply(lambda x:x.split('.')[0])
    securities_coll.insert_many(df_db.to_dict('records'))
    QA_util_log_info("QA_SU_save_securities:{},count:{}".format(current_date,str(len(df_db)))) 
def _get_fundamentals_to_db(collection,current_date):

    df = jqdatasdk.get_fundamentals(jqdatasdk.query(
            jqdatasdk.valuation 
        ).filter(

        ), date=current_date)
    df_db=df.rename(columns={"code":"code_jd","day":"date"})
    df_db['code']=df_db['code_jd'].apply(lambda x:x.split('.')[0])
    collection.insert_many(df_db.to_dict('records'))
    QA_util_log_info("QA_SU_save_fundamentals:{},count:{}".format(current_date,str(len(df_db)))) 

def _get_trade_days(coll,act):
    _first_date='2015-01-02'
    _last_date='2015-01-02'
    try:
        _first_date=coll.find_one(sort=[('date',1)])['date']
    except Exception as e:
        QA_util_log_expection("QA_SU_save_{}==error:{}".format(act,str(e)))
        return[]
    try:
        _last_date=coll.find_one(sort=[('date',-1)])['date']
    except Exception as e:
        QA_util_log_expection("QA_SU_save_{}==error:{}".format(act,str(e)))
        return[]
    QA_util_log_info("QA_SU_save_{}:first_date:{} & last_date:{}".format(act,_first_date,_last_date))

    last_real_date= str(now_time())[0:10]
    
    if(last_real_date==_last_date):
        QA_util_log_info('QA_SU_save_{}，不需要更新'.format(act))
        return[]

    trade_days=QA_util_get_trade_range(
        trade_date_sse[trade_date_sse.index(_last_date) + 1],
        last_real_date)
    return trade_days

def QA_SU_save_fundamentals(client=DATABASE, ui_log=None, ui_progress=None):
    
    QA_util_log_info("QA_SU_save_fundamentals==========begin")
    coll=client.jqData_fundamentals
    
    trade_days=_get_trade_days(coll,'fundamentals')
    try:
        for day in trade_days:
            _get_fundamentals_to_db(coll,day)
        #get_all_securities_to_db(day)
        
    except Exception as e:
        QA_util_log_expection("QA_SU_save_fundamentals==error:{}".format(str(e)))

    QA_util_log_info("QA_SU_save_fundamentals==========end")
def QA_SU_save_securities(client=DATABASE, ui_log=None, ui_progress=None):
    
    QA_util_log_info("QA_SU_save_securities==========begin")
    coll=client.jqData_securities
    
    trade_days=_get_trade_days(coll,'securities')
    try:
        for day in trade_days:
            _get_securities_to_db(coll,day)
        #get_all_securities_to_db(day)
        
    except Exception as e:
        QA_util_log_expection("QA_SU_save_securities==error:{}".format(str(e)))

    QA_util_log_info("QA_SU_save_securities==========end")

if __name__ == "__main__":
    #QA_SU_save_stock_min()
    collection = DATABASE['jqData_fundamentals']  #valuation
    securities_coll= DATABASE['jqData_securities']  #valuation
    start_date='2018-01-02'
    end='2018-04-30'
    trade_days=QA_util_get_trade_range(start_date,end)
    for day in trade_days:
        try:
            _get_fundamentals_to_db(collection,day)
            _get_securities_to_db(securities_coll,day)
            print(day+" success")
        except Exception as e:
            print(day+" error")
            print(str(e))
            break
    pass
