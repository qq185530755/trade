from pymongo import UpdateOne, MongoClient
from bson.objectid import ObjectId
import log

LOG = log.init_logger(__name__, log_path='test1.log')
# DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['firstdb']
DB_CONN1 = MongoClient('mongodb://127.0.0.1:27017')['daily_stocks']
# DB_CONN2 = MongoClient('mongodb://127.0.0.1:27017')['test']
DB_CONN3 = MongoClient('mongodb://127.0.0.1:27017')['pe_db']
# pe_col = DB_CONN3['2015pe']
# col_tradeday = DB_CONN['tradeday']
# col_stocklist = DB_CONN['stocklist']
col_hfq_daily = DB_CONN1['605338-688011daily_hfq']
# col_test = DB_CONN2['test']


class DNINFO:
    DB_CONN = MongoClient('mongodb://127.0.0.1:27017')

    def get_all_datebases_and_collections(self):
        # 获取到mongodb中所有的数据库和数据集
        all_db = self.DB_CONN.list_database_names()   # 获取到mongodb中所有的数据库名称
        print('all databases: %s' % all_db)
        for db in all_db:
            cols = self.DB_CONN[db].list_collection_names()   # 获取每个数据库中的所有数据集名称
            print('all cols in %s:\n%s\n' %(db, cols))

    def get_data_for_col(self, db, col):
        # 查看某个数据库的数据集中的所有数据
        datas = self.DB_CONN[db][col].find({'code':'603039', 'date':{'$in': ['20170117', '20170116']}})
        print(datas[0])
        for i in datas:
            print(i)

    def delete_database(self, database_name):
        # 删除数据库，谨慎使用
        self.DB_CONN.drop_database(database_name)


# DNINFO().delete_database('')     #删除数据库，谨慎使用
DNINFO().get_all_datebases_and_collections()   #获取所有数据库和集合

DNINFO().get_data_for_col('daily_stocks', '603018-603039daily_bfq')   #查看集合中的数据
# s=[]
# s.append(UpdateOne(
#     {'_id': ObjectId('6000560b57a90164160de1b4')},
#     {'$set': {'code2': 7}}
# ))
# s.append(UpdateOne(
#     {'_id': ObjectId('6000561e57a90164160de1bb')},
#     {'$set': {'code2': 7}}
# ))
# DNINFO().DB_CONN['test']['test'].bulk_write(s, ordered=False)
# DNINFO().get_data_for_col('firstdb', 'stocklist')


# x = col.delete_many({})  # 谨慎使用，千万不要删错数据集
# print(col_stocklist.name)
# for i in col_stocklist.find({'symbol': {'$gt': '000049'}}):
#     LOG.info(i)

# for i in col_hfq_daily.find({}):
#     print(i)

# LOG.info(col_hfq_daily.find_one({}))

# doc={}
# doc['code'] = ['11','22']
# update_requests = []   #写入数据库
# update_requests.append(
#                 UpdateOne(
#                     {'code': doc['code']},
#                     {'$set': doc},
#                     upsert=True)
#             )
# update_result = col_test.bulk_write(update_requests, ordered=False)
# print(list(col_test.find({})))
