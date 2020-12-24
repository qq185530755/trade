from datetime import datetime
import tushare as ts
from pymongo import MongoClient
# a = datetime.now().strftime('%Y-%m-%d')
# data = ts.get_k_data('000001', index=True, start='2020-12-22', end='2020-12-23')
# print(data.index)
# print(dict(data.loc[235]))
# for i in data.index:
#     print(i)
# print(data)
# print(type(data))

client = MongoClient('mongodb://127.0.0.1:27017')
my = client['first']
col = my['table2']
db_data = [
{'num2': 2, 'time2': '11.22'},
{'num2': 3, 'time2': '11.33'}
]
x = col.insert_many(db_data)

coll = my['table1']
print(type(coll))
d = {'num': 2}
print(coll.delete_many(d))
db = client.list_database_names()
print(db)
col.drop()