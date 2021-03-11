#  -*- coding: utf-8 -*-

import tushare as ts
import log
from pymongo import UpdateOne, MongoClient
LOG = log.init_logger(__name__,log_path='all.log')

DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['firstdb']
col = DB_CONN['stocklist']

ts.set_token('727538a5358d4bb61b813c9ae2ce2be55ace4d8f529654fa52e039b6')
# index_codes = ['000001.SH', '000300.SH', '399001.SZ', '399005.SZ', '399006.SZ']
# start = '20141008'
# end = '20201228'
df = ts.pro_bar(ts_code='688698.SH', adj='hfq', start_date='20141009', end_date='20201228', ma=[5,10])

#pro = ts.pro_api()
# df = pro.daily_basic(ts_code='', trade_date='20201123', fields='ts_code,trade_date,turnover_rate,pe')
# df = df[df.pe>0].sort_values(by='pe').head(100)

#df = pro.adj_factor(ts_code='002285.SZ', start_date='20200506', end_date='20210106') #获取复权因子

# pro = ts.pro_api()
# df = pro.index_basic(market='SZSE')

# data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

# df = pro.daily_basic(ts_code='', trade_date='20210112', fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pe_ttm,pb') #获取股票基本信息
for i in df.index:
    print(dict(df.loc[i]))


# update_requests = []
# for index in data.index:
#     info = dict(data.loc[index])
#     update_requests.append(
#         UpdateOne(
#             {'symbol': info['symbol'], 'ts_code': info['ts_code']},
#             {'$set': info},
#             upsert=True)
#     )
# if len(update_requests) > 0:
#     # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
#     update_result = col.bulk_write(update_requests, ordered=False)
#     print('保存日线数据， 插入：%4d条, 更新：%4d条' %
#           (update_result.upserted_count, update_result.modified_count))
