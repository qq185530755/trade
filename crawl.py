from pymongo import UpdateOne, MongoClient
import tushare as ts
from datetime import datetime
import daily_stock_data
import log

DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['firstdb']

LOG = log.init_logger(__name__)


class DailyCrawler(object):

    def __init__(self):
        ts.set_token('727538a5358d4bb61b813c9ae2ce2be55ace4d8f529654fa52e039b6')
        self.pro = ts.pro_api()
        self.data = daily_stock_data.StockData()

    def get_trade_day(self, start, end):
        # 如果已存数据库，就不需要该接口
        cal = self.pro.trade_cal(exchange='', start_date=start, end_date=end)
        col_trade = DB_CONN['tradeday']
        update_requests = []
        for index in cal.index:
            date = dict(cal.loc[index])
            date['is_open'] = int(date['is_open'])
            if date['is_open']:
                update_requests.append(
                    UpdateOne(
                        {'date': date['cal_date']},
                        {'$set': date},
                        upsert=True)
                )
        if len(update_requests) > 0:
            update_result = col_trade.bulk_write(update_requests, ordered=False)
            LOG.info('save trade day to mongodb success')

    def get_all_stocks(self):
        # 如果已存数据库，就不需要该接口了
        data = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        col_stocks = DB_CONN['stocklist']
        update_requests = []
        for index in data.index:
            info = dict(data.loc[index])
            update_requests.append(
                UpdateOne(
                    {'symbol': info['symbol'], 'ts_code': info['ts_code']},
                    {'$set': info},
                    upsert=True)
            )
        if len(update_requests) > 0:
            # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
            update_result = col_stocks.bulk_write(update_requests, ordered=False)
            LOG.info('save stocks to mongodb success')

    def crawl_adj_factor_save_into_bufuquan(self,begin_date=None, end_date=None, finish_code=None):
        col_stocks = DB_CONN['stocklist']
        filters = {}
        if finish_code:
            filters = {'symbol': {'$gt': finish_code}}

        all_stocks = col_stocks.find(filters, no_cursor_timeout=True)
        for stock in all_stocks:
            # 抓取前复权的价格
            code = stock['ts_code']

            for n in range(5):
                try:
                    # 交易日里本来存在复权数据，可能因为网络问题，获取漏了某个交易日的复权因子数据，或者df数据为空，导致从字典中取漏掉交易日的复权数据，会报错键不存在。
                    df_adj = self.pro.adj_factor(ts_code=code, start_date=begin_date, end_date=end_date)
                    date_relate_adj = {}  # 股票在交易日那天对应的复权因子
                    for df_index in df_adj.index:
                        # 将DataFrame中的一行数据转dict
                        doc = dict(df_adj.loc[df_index])
                        date_relate_adj[doc['trade_date']] = doc['adj_factor']
                    col = self.data.get_stocks_col(code[:6], 'bfq')
                    stock_all_tradeday_data = col.find({'code': code[:6]})    # 每个股票在数据库中的所有不复权的数据
                    print('get %s data success on ts' % code)
                    update_requests = []
                    for data in stock_all_tradeday_data:
                        date = data['date']
                        update_requests.append(
                            UpdateOne(
                                {'code': code[:6], 'date': date},
                                {'$set': {'adj_factor': date_relate_adj[date]}},
                                upsert=True)
                        )
                    break
                except Exception as e:
                    print('%s times error happen on %s' % (n+1, code))
                    print(df_adj)
                    LOG.error(e)
            else:
                raise KeyError


            if len(update_requests) > 0:
                # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
                update_result = col.bulk_write(update_requests, ordered=False)
                LOG.info('save daily data in %s success，code： %s, insert：%d, update：%d'
                         % (col.name, code, update_result.upserted_count, update_result.modified_count))
                if update_result.upserted_count:
                    raise ValueError('upsert error')


    def crawl_ma_save_into_houfuquan(self,begin_date=None, end_date=None, finish_code=None):
        col_stocks = DB_CONN['stocklist']
        filters = {}
        if finish_code:
            filters = {'symbol': {'$gt': finish_code}}

        all_stocks = col_stocks.find(filters, no_cursor_timeout=True)
        for stock in all_stocks:
            # 抓取前复权的价格
            code = stock['ts_code']

            for n in range(5):
                try:
                    # 交易日里本来存在复权数据，可能因为网络问题，获取漏了某个交易日的复权因子数据，或者df数据为空，导致从字典中取漏掉交易日的复权数据，会报错键不存在。
                    #df_adj = self.pro.adj_factor(ts_code=code, start_date=begin_date, end_date=end_date)
                    df_ma= ts.pro_bar(ts_code=code, adj='hfq', start_date=begin_date, end_date=end_date, ma=[5, 10])
                    date_relate_ma = {}  # 股票在交易日那天对应的复权因子
                    for df_index in df_ma.index:
                        # 将DataFrame中的一行数据转dict
                        doc = dict(df_ma.loc[df_index])
                        date_relate_ma[doc['trade_date']] = (doc['ma5'], doc['ma10'])
                    col = self.data.get_stocks_col(code[:6], 'hfq')
                    stock_all_tradeday_data = col.find({'code': code[:6]})    # 每个股票在数据库中的所有后复权的数据
                    print('get %s data success on ts' % code)
                    update_requests = []
                    for data in stock_all_tradeday_data:
                        date = data['date']
                        update_requests.append(
                            UpdateOne(
                                {'code': code[:6], 'date': date},
                                {'$set': {'ma5': date_relate_ma[date][0], 'ma10': date_relate_ma[date][1]}},
                                upsert=True)
                        )
                    break
                except Exception as e:
                    print('%s times error happen on %s' % (n+1, code))
                    print(df_ma)
                    LOG.error(e)
            else:
                raise KeyError


            if len(update_requests) > 0:
                # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
                update_result = col.bulk_write(update_requests, ordered=False)
                LOG.info('save daily data in %s success，code： %s, insert：%d, update：%d'
                         % (col.name, code, update_result.upserted_count, update_result.modified_count))
                if update_result.upserted_count:
                    raise ValueError('upsert error')


    def crawl_fuquan(self, begin_date=None, end_date=None, finish_code=None):
        """
        TODO 遗漏了5 10均线数据，在其他机器运行要补上
        抓取股票的日K数据，包含了前复权和后复权两种
        :param finish_code: 数据已保存到数据集中股票的代码
        :param begin_date: 开始日期
        :param end_date: 结束日期
        """
        col_stocks = DB_CONN['stocklist']

        # 当前日期
        now = datetime.now().strftime('%Y%m%d')

        # 如果没有指定开始日期，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日期，则默认为当前日期
        if end_date is None:
            end_date = now

        filters = {}
        if finish_code:
            filters = {'symbol': {'$gt': finish_code}}

        all_stocks = col_stocks.find(filters, no_cursor_timeout=True)
        for stock in all_stocks:
            # 抓取前复权的价格
            code = stock['ts_code']
            # df_daily_qfq = ts.pro_bar(ts_code=code, adj='qfq', start_date=begin_date, end_date=end_date)
            # self.data.save_stocks_col(code[:6], df_daily_qfq, 'qfq',  {'index': False})

            # 抓取后复权的价格
            df_daily_hfq = ts.pro_bar(ts_code=code, adj='hfq', start_date=begin_date, end_date=end_date)
            print('get %s data success on ts' % code)
            self.data.save_stocks_col(code[:6], df_daily_hfq, 'hfq', {'index': False})


    def crawl_bufuquan(self, begin_date=None, end_date=None, finish_code=None):
        """
        TODO 该函数抓取数据时遗漏了复权因子数据，在其他机器上运行需要重改代码，加上复权因子，然后保存
        抓取股票的日K数据，包含了前复权和后复权两种
        :param finish_code: 数据已保存到数据集中股票的代码
        :param begin_date: 开始日期
        :param end_date: 结束日期
        """
        col_stocks = DB_CONN['stocklist']

        # 当前日期
        now = datetime.now().strftime('%Y%m%d')

        # 如果没有指定开始日期，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日期，则默认为当前日期
        if end_date is None:
            end_date = now
        filters = {}
        if finish_code:
            filters = {'symbol': {'$gt': finish_code}}
        all_stocks = col_stocks.find(filters, no_cursor_timeout=True)
        print(all_stocks)
        for stock in all_stocks:
            # 抓取前复权的价格
            code = stock['ts_code']
            # df_daily_qfq = ts.pro_bar(ts_code=code, adj='qfq', start_date=begin_date, end_date=end_date)
            # self.data.save_stocks_col(code[:6], df_daily_qfq, 'qfq',  {'index': False})

            # 抓取后复权的价格
            df_daily_bfq = ts.pro_bar(ts_code=code, start_date=begin_date, end_date=end_date)
            print('get %s data success on ts' % code)
            self.data.save_stocks_col(code[:6], df_daily_bfq, 'bfq', {'index': False})

    def crawl_index(self, begin_date=None, end_date=None):
        """
        TODO 遗漏了均线数据，在其他机器运行需要补上
        抓取指数的日K数据。
        指数行情的主要作用：
        1. 用来生成交易日历
        2. 回测时做为收益的对比基准

        :param begin_date: 开始日期
        :param end_date: 结束日期
        """

        # 指定抓取的指数列表，可以增加和改变列表里的值
        index_codes = ['000001', '000300', '399001', '399005', '399006']

        # 当前日期
        now = datetime.now().strftime('%Y-%m-%d')
        # 如果没有指定开始，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日，则默认为当前日期
        if end_date is None:
            end_date = now

        # 按照指数的代码循环，抓取所有指数信息
        col_index = DB_CONN['index']
        for code in index_codes:
            # 抓取一个指数的在时间区间的数据
            df_daily = ts.get_k_data(code, index=True, start=begin_date, end=end_date)
            # 保存数据
            self.save_data(code, df_daily, col_index, {'index': True})

    def save_data(self, code, df_daily, collection, extra_fields=None):
        """
        将从网上抓取的数据保存到本地MongoDB中

        :param code: 股票代码
        :param df_daily: 包含日线数据的DataFrame
        :param collection: 要保存的数据集
        :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
        """

        # 数据更新的请求列表
        update_requests = []

        # 将DataFrame中的行情数据，生成更新数据的请求
        for df_index in df_daily.index:
            # 将DataFrame中的一行数据转dict
            doc = dict(df_daily.loc[df_index])
            # 设置股票代码
            doc['code'] = code[:6]

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
            LOG.info('save daily data success，code： %s, insert：%4d, update：%4d' % (
            code, update_result.upserted_count, update_result.modified_count))


if __name__ == '__main__':
    start = '20141008'
    end = '20201228'
    DailyCrawler().crawl_ma_save_into_houfuquan(start, end, '688679')

