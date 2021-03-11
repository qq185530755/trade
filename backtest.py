from pymongo import UpdateOne, MongoClient
from daily_stock_data import StockData
import pandas as pd
from daily_stock_data import StockData
import log

LOG = log.init_logger(__name__, log_path='test.log')

def is_k_up_break_ma10(code, date):
    # 判断是否穿过10日均线， 1表示上穿， -1表示下穿， 0表示不上穿也不是下穿
    col = StockData().get_stocks_col(code, 'hfq')
    stock_datas = col.find({'code':code, 'date':{'$lte':date}}, limit=2)  #判断上下穿需要当日数据和前一日的数据
    cur_stock_data = stock_datas[0]
    last_stock_data = stock_datas[1]
    if cur_stock_data['close'] > cur_stock_data['ma10'] and last_stock_data['close'] <= last_stock_data['ma10']:
        return 1
    if cur_stock_data['close'] < cur_stock_data['ma10'] and last_stock_data['close'] >= last_stock_data['ma10']:
        return -1
    return 0

def get_adjust_day_stock(begin_date, end_date):
    # 获取股票池调整日的所有股票
    col = MongoClient('mongodb://127.0.0.1:27017')['pe_db']['2015pe_new']
    datas = col.find()
    d={}
    for data in datas:
        d[data['phase']] = data['stocks']
    return d

def adjust_stock_volume_by_aufactor():
    # 根据复权因子，调整股票持仓的数量， 复权因子变化后，股价会变化，相应的持仓数也会变化
    pass

def backtest(begin_date, end_date):
    index_col = MongoClient('mongodb://127.0.0.1:27017')['firstdb']['daily_index']
    #获取指定时间段内的交易日
    trade_days = index_col.find({'code':'000300', 'trade_date': {'$gte':begin_date , '$lte': end_date}}, sort=[('trade_date', 1)])
    init_cash = 2000000    #初始总仓位
    single_positon = 100000  #购买一只票的仓库
    df_profit = pd.DataFrame(columns=['net_value', 'profit', 'hs300'])
    first_day_hs300 = trade_days[0]['close']  #hs300第一个交易日收盘的值
    date_code_dict = get_adjust_day_stock(begin_date, end_date)  #获取调整日，以及对应的股票池
    this_phase_stock_pool = []
    last_phase_stock_pool = []
    need_buy_stocks = set()
    need_sell_stocks = set()

    holding_stocks_dict = {}  # 包含的字段  买入时的成本， 买入时的数量 ,股票的净值
    last_tradeday = None

    for each_date in trade_days:
        date = each_date['trade_date']
        print('tradeday is %s' % date)
        holding_codes = holding_stocks_dict.keys()  # 获取持仓股票的代码信息

        if last_tradeday and len(holding_codes)>0:
            for code in holding_codes:
                col = StockData.get_stocks_col(code, 'bfq')
                data = col.find({'code': code, 'date': {'$lte':date}}, limit=2)
                if data[0]['date'] != date:  #如果不相等， 说明在当前交易日找不到股票的数据，就说明股票停牌了，也就不需要计算复权因子
                    print('%s stop at %s' %(code, date))
                    continue
                cur_aufactor = date[0]['adj_factor']
                last_aufactor = date[1]['adj_factor']
                if cur_aufactor != last_aufactor:
                    last_volume = holding_stocks_dict[code]['volume']
                    current_volume = int(last_volume * (cur_aufactor / last_aufactor))
                    holding_stocks_dict[code]['volume'] = current_volume
                    print('%s acfactor changed at %s' %(code, date))


        if need_sell_stocks:   # 必须先卖后买，这样可以充分利用资金，所以第一步应该根据复权因子更新持仓数量
            print('need to sell stocks is %s' % need_sell_stocks)
            sell_failed = set()   # 本来要卖出，却因为停牌未能卖出， 就将其加入下个交易日要卖出的票， 直到复盘就卖出
            for code in need_sell_stocks:
                col = StockData.get_stocks_col(code, 'bfq')
                data = col.find({'code': code, 'date':date})
                if date:  # 根据下穿均线来判断是否卖出，但卖出时是在判断后的下一个交易日，所以必须保证没有停牌才能卖出。
                    open = data['open']
                    sell_amout = holding_codes[code]['volume'] * open
                    init_cash += sell_amout

                    cost = holding_codes[code]['cost']  #股票的成本
                    single_profit = sell_amout/cost - 1
                    print('sell stock code %s, sell_price %s, cost %s, profit %s'%(code, sell_amout, cost, single_profit))
                    holding_stocks_dict.pop(code)
                else:
                    sell_failed.add(code)
            need_sell_stocks = sell_failed  #如果存在停牌未能卖出的票，移到下个交易日处理，如果不存在，赋值成空集合
            if need_sell_stocks:
                print('%s stop, can not sell' %need_sell_stocks)

        if need_buy_stocks:
            print('need to buy stocks is %s' % need_buy_stocks)
            for code in need_buy_stocks:
                if init_cash >= single_positon:
                    col = StockData.get_stocks_col(code, 'bfq')
                    data = col.find({'code': code, 'date': date})
                    if date:  # 如果要买的票 没开盘，就放弃不买了
                        open = data['open']
                        volume = int(single_positon/open/100) * 100
                        cost = volume * open
                        holding_stocks_dict[code] = {'cost': cost, 'volume': volume}
                        init_cash -= cost
                        print('buy stock code %s, cost %s, volume %s' % (code, cost, volume))
                else:
                    break
            print('%s money left in %s' % (init_cash, date))
            need_buy_stocks = set()

        if date in date_code_dict:
            print('stock pool adjust date %s' % date)
            this_phase_stock_pool = date_code_dict[date]
            for ts_code in last_phase_stock_pool:
                if ts_code not in this_phase_stock_pool and ts_code[:6] in holding_codes:
                    need_sell_stocks.add(ts_code[:6])
                    print('%s not in this_phase_stock_pool, need sell' %ts_code)
            last_phase_stock_pool = this_phase_stock_pool

        for code in holding_stocks_dict:
            flag = is_k_up_break_ma10(code, date)
            if flag == -1:
                need_sell_stocks.add(code)

            col = StockData().get_stocks_col(code, 'bfq')
            stock_datas = col.find({'code': code, 'date': date})
            if stock_datas:
                holding_stocks_dict[code]['close'] = stock_datas['close']  # 对每个持有的票记录收盘价，方便统计持有股票的市值， 收盘价需要不复权的

        for code in this_phase_stock_pool:
            if code not in holding_stocks_dict:
                flag = is_k_up_break_ma10(code, date)
                if flag == 1:
                    need_buy_stocks.add(code)

        total_value = 0
        for code in holding_stocks_dict:
            value = holding_stocks_dict[code]['close'] * holding_stocks_dict[code]['volume'] #单只票的市值
            total_value += value    #计算总市值
            #计算单只票的收益， 先省略

        total_money = init_cash + total_value
        cur_hs300 = each_date['close']
        last_tradeday = date

        df_profit.loc[date] = {
            'net_value': round(total_money/2000000, 3),
            'profit': 100*round(total_money/2000000 - 1, 3),
            'hs300': 100 * round(cur_hs300/first_day_hs300 - 1, 3)
        }
        print('%s total profit is %s'%(date, 100*round(total_money/2000000 - 1, 3)))
        print('%s hs300 change is %s' % (date, 100 * round(cur_hs300/first_day_hs300 - 1, 3)))

    for i in df_profit.index:
        LOG.info(dict(df_profit.loc[i]))















backtest('20150105', '20151231')
