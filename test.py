import copy
import tushare as ts
import log
import pandas as pd
from daily_stock_data import StockData
from pymongo import UpdateOne, MongoClient
LOG = log.init_logger(__name__, log_path='test.log')

DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['firstdb']
tradeday_col = DB_CONN['tradeday']
index_col = DB_CONN['daily_index']
DB_CONN1 = MongoClient('mongodb://127.0.0.1:27017')['pe_db']
pe_col = DB_CONN1['2015pe_new']
DB_CONN2 = MongoClient('mongodb://127.0.0.1:27017')['daily_stocks']
hfq_col = DB_CONN['hfq_daily']

# for i in tradeday_col.find():
#     print(i, flush=True)
ts.set_token('727538a5358d4bb61b813c9ae2ce2be55ace4d8f529654fa52e039b6')
pro = ts.pro_api()


def save_index_data():
    collection = DB_CONN['daily_index']
    index_codes = ['000001.SH', '000300.SH', '399001.SZ', '399005.SZ', '399006.SZ']
    for code in index_codes:
        df = ts.pro_bar(ts_code=code, asset='I', start_date='20141008', end_date='20201228')
        update_requests = []
        for i in df.index:
            doc = dict(df.loc[i])
            doc['ts_code'] = doc['ts_code'][:6]
            update_requests.append(
                UpdateOne(
                    {'code': doc['ts_code'], 'trade_date': doc['trade_date']},
                    {'$set': doc},
                    upsert=True)
            )
        if len(update_requests) > 0:
            # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
            update_result = collection.bulk_write(update_requests, ordered=False)
            LOG.info('save index data in %s success，code： %s, insert：%d, update：%d'
                     % (collection.name, code, update_result.upserted_count, update_result.modified_count))

def save_pe_stock_col(date, codes):
    """
    将从网上抓取的数据保存到本地MongoDB中
    :param date: 交易周期的第一天
    :param codes: 第一天的100个满足PE条件的股票， list类型
    """
    # 数据更新的请求列表
    update_requests = []

    # 将DataFrame中的行情数据，生成更新数据的请求
    doc = {}
    doc['phase'] = date
    doc['stocks'] = codes

    update_requests.append(
        UpdateOne(
            {'phase': doc['phase']},
            {'$set': doc},
            upsert=True)
    )

    # 如果写入的请求列表不为空，则保存都数据库中
    if len(update_requests) > 0:
        # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
        update_result = pe_col.bulk_write(update_requests, ordered=False)
        LOG.info('save pe data in %s success，date： %s, insert：%d, update：%d'
                 % (pe_col.name, date, update_result.upserted_count, update_result.modified_count))


def get_pe_by_tradedays(start, end):
    # 在指定时间段内获取前100PE的股票

    tradedays = tradeday_col.find({'cal_date': {'$gte': start, '$lte': end}}, projection={'date': True, '_id': False})
    tradedays = list(tradedays)
    last_date = tradedays[0]['date']   #第一次获取PE时 以第一天来取
    length = len(tradedays)
    print(tradedays)
    for i in range(0, length, 7):
        first_date = tradedays[i:i + 7][0]['date']
        print(first_date)
        first_date_codes = StockData().get_all_codes_in_tradeday(first_date)  #找出换股日 当日是开盘的股票
        df = pro.daily_basic(ts_code='', trade_date=last_date,
                             fields='ts_code,trade_date,turnover_rate,pe')
        pe = df[df.pe>0].sort_values(by='pe').head(150)
        cur_phase_codes = []
        for n in pe.index:
            ts_code = pe.loc[n, 'ts_code']
            if ts_code in first_date_codes:   # PE排序 是选取的 换股日的前一日的股票， 所以必须保证换股前一日的股票， 在换股日必须是开盘日， 否则不能购买，选出来没有意义
                cur_phase_codes.append(ts_code)
            if len(cur_phase_codes) == 100:
                break
        else:
            raise ValueError('need 100 stocks')
        print(cur_phase_codes)
        last_date = tradedays[i:i+7][-1]['date']   #当前阶段的最后一个交易日，作为下个阶段获取PE的日期
        save_pe_stock_col(first_date, cur_phase_codes)

def get_stocks_price_for_pool(stocks_pool, tradeday, last_stocks_info={}):
    data = StockData()
    stocks_info = {}
    for code in stocks_pool:
        stock_data = data.get_stock_tradeday_info(code[:6], tradeday, 'hfq')
        if stock_data:      # 股票池的票，如果在当前交易周期的第一个交易日有数据，就表示没有停牌， 获取到开盘价，与上个交易周期进行对比，计算收益
            stocks_info[code] = [stock_data['open'], 1]
        else:
            if code in last_stocks_info and last_stocks_info[code][1]==0:  # 上一个交易周期就停牌了， 到当前交易周期依然停牌的
                stocks_info[code] = last_stocks_info[code]
                LOG.info('last phare code:%s have stoped' %code)
                continue
            dates = tradeday_col.find({'date':{'$lt': tradeday}}, sort=[('date', -1)], limit=7, projection={'date': True, '_id':False})
            for date in dates:
                stock_data = data.get_stock_tradeday_info(code[:6], date['date'], 'hfq')  # 上个交易周期就停牌的票，在上面已经剔除，这个周期才停牌的，向前找6个交易日
                if stock_data:                                                            # 一定可以找到有最后一个开盘的交易日，获取最后一个开盘交易日的收盘价，作为现在的价格计算收益
                    stocks_info[code] =[stock_data['close'], 0]
                    break
            else:
                raise ValueError('code:%s stop in each tradeday of current phare')
            LOG.info('current phare code:%s stoped, stopday:%s' % (code, date['date']))
    return stocks_info

def compute_stocks_profit():
    df_profit = pd.DataFrame(columns=['PeStokcs', 'hs300'])
    phase_and_stocks = pe_col.find({}) #获取每个阶段第一天的日期 以及前100PE的股票
    stocks_profit = 0
    last_stocks_valus = 1
    last_stocks_pool = phase_and_stocks[0]['stocks']  # 真正计算收益是从 第二个交易周期开始计算的， 所以第一个交易周日都可作为上一次的股票池和上一次股票价格
    last_tradeday = phase_and_stocks[0]['phase']
    last_stocks_info = get_stocks_price_for_pool(last_stocks_pool, last_tradeday)  #第一个交易周日没有停牌的票，所有票都必须是开盘的
    index_porfit = 0
    last_index_valus = index_col.find_one({'code':'000300', 'trade_date':phase_and_stocks[0]['phase']})['open']
    df_profit.loc[last_tradeday] = {'PeStokcs':stocks_profit, 'hs300':index_porfit}
    for i in range(1, pe_col.count_documents({})):
        phase_profit = 0
        cur_tradeday = phase_and_stocks[i]['phase']
        LOG.info('start computer phase:%s profit\nlast phase stocks pool:%s' % (cur_tradeday, last_stocks_pool))
        cur_old_stocks_info = get_stocks_price_for_pool(last_stocks_pool, cur_tradeday, last_stocks_info)
        cur_old_stop_stocks = []
        for code in last_stocks_pool:
            try:
                profit = float(format(cur_old_stocks_info[code][0]/last_stocks_info[code][0] - 1, '.2f'))
            except Exception as e:
                print('last_stocks_pool: %s' %last_stocks_pool)
                print('cur_old_stocks_info: %s' %cur_old_stocks_info)
                print('last_stocks_info: %s' % last_stocks_info)
                print(e)
                raise KeyError
            phase_profit = phase_profit + profit
            if cur_old_stocks_info[code][1] == 0:
                cur_old_stop_stocks.append(code)
        cur_stocks_valus = last_stocks_valus + phase_profit/100
        stocks_profit = (cur_stocks_valus - 1)*100
        cur_index_valus = index_col.find_one({'code': '000300', 'trade_date': phase_and_stocks[i]['phase']})['open']
        index_porfit = (cur_index_valus/last_index_valus -1)*100
        df_profit.loc[cur_tradeday] = {'PeStokcs': stocks_profit, 'hs300': index_porfit}

        cur_stocks_pool = cur_old_stop_stocks + phase_and_stocks[i]['stocks'][:100-len(cur_old_stop_stocks)]
        cur_new_stocks_info = {}
        cur_new_stocks_pool = []
        for code in cur_stocks_pool:
            if code not in cur_old_stocks_info:
                cur_new_stocks_pool.append(code)
            else:
                cur_new_stocks_info[code] = cur_old_stocks_info[code]
        find_new_stocks_info = get_stocks_price_for_pool(cur_new_stocks_pool, cur_tradeday)
        cur_new_stocks_info.update(find_new_stocks_info)
        last_stocks_pool = cur_stocks_pool
        last_stocks_info = cur_new_stocks_info
        LOG.info('cur_new_stocks_info length is %s' % len(cur_new_stocks_info))

if __name__ == "__main__":
    # get_pe_by_tradedays('20150105', '20151231')   #获取每个阶段前100PE，执行过一次保存到数据库， 后续就不需要执行了
    compute_stocks_profit()

