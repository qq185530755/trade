from pymongo import UpdateOne, MongoClient
import log

LOG = log.init_logger(__name__)
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['daily_stocks']
StockList = MongoClient('mongodb://127.0.0.1:27017')['firstdb']['stocklist']


class StockData:
    def __init__(self):
        self.codes = self.get_stocks_code()

    def get_stocks_code(self):
        codes = []
        for code in StockList.find():
            codes.append(code['symbol'])
        return codes

    def get_stocks_col(self, code, daily_type):
        # each col contains 20 stocks
        for i in range(0, len(self.codes), 20):
            col_name = {'start': self.codes[i:i + 20][0], 'end': self.codes[i:i + 20][-1]}
            if col_name['start'] <= code <= col_name['end']:
                name = col_name['start'] + '-' + col_name['end'] + 'daily_' + daily_type
                break
        return DB_CONN[name]

    def get_stock_tradeday_info(self, code, date, daily_type):
        # 获取股票在某个交易日的具体信息

        col = self.get_stocks_col(code, daily_type)
        return col.find_one({'code':code, 'date':date})

    def get_all_codes_in_tradeday(self, date):
        # 在数据中 查找某个交易日 所有开盘的股票代码
        cols = DB_CONN.list_collection_names()
        codes = []
        for col in cols:
            stocks = DB_CONN[col].find({'date': date})
            for stock in stocks:
                codes.append(stock['ts_code'])
        return codes

    def save_stocks_col(self, code, df_daily, daily_type, extra_fields=None):
        """
        将从网上抓取的数据保存到本地MongoDB中

        :param daily_type: 复权类型
        :param code: 股票代码
        :param df_daily: 包含日线数据的DataFrame
        :param collection: 要保存的数据集
        :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
        """

        collection = self.get_stocks_col(code, daily_type)
        # 数据更新的请求列表
        update_requests = []

        # 将DataFrame中的行情数据，生成更新数据的请求
        for df_index in df_daily.index:
            # 将DataFrame中的一行数据转dict
            doc = dict(df_daily.loc[df_index])
            # 设置股票代码
            doc['code'] = code

            # 如果指定了其他字段，则更新dict
            if extra_fields is not None:
                doc.update(extra_fields)

            # 生成一条数据库的更新请求
            # 注意：
            # 需要在code、date、index三个字段上增加索引，否则随着数据量的增加，
            # 写入速度会变慢，创建索引的命令式：
            # db.daily.createIndex({'code':1,'date':1,'index':1})
            update_requests.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['trade_date'], 'index': doc['index']},
                    {'$set': doc},
                    upsert=True)
            )
        # 如果写入的请求列表不为空，则保存都数据库中
        if len(update_requests) > 0:
            # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
            update_result = collection.bulk_write(update_requests, ordered=False)
            LOG.info('save daily data in %s success，code： %s, insert：%d, update：%d'
                     % (collection.name, code, update_result.upserted_count, update_result.modified_count))

if __name__ == '__main__':
    # print(StockData().get_all_codes_in_tradeday('20201124'))

    dates = ['20201022', '20201023', '20201026', '20201027']
    for date in dates:
        print(StockData().get_stock_tradeday_info('002775', date, 'bfq'))


