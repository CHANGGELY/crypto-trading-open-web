import json
import math
import time
import traceback
from datetime import timedelta

import ccxt
import pandas as pd
from tqdm import tqdm

from common.utils import retry_wrapper

exchange = ccxt.binance()
exchange.https_proxy = 'http://127.0.0.1:7890/'
exchange.apiKey = ''
exchange.secret = ''
exchange.timeout = 3000

def fetch_account_balance(enable_retry=False):
    """
    获取当前账户余额，不包含未实现盈亏
    :param enable_retry:
    :return:
    """
    account_data = retry_wrapper(
        exchange.fapiPrivateV2GetAccount,
        func_name='fapiPrivateV2GetAccount',
        enable_retry=enable_retry,
    )
    assets = pd.DataFrame(account_data['assets'])
    return float(assets[assets['asset'] == 'USDT']['walletBalance'])


def fetch_positions(enable_retry=False):
    """
    获取当前账户所有持仓信息，返回以 symbol 为索引的 DataFrame
    :param enable_retry:
    :return:

                  当前持仓量   持仓均价   当前价格  持仓收益
        symbol
        OXTUSDT       -961.0   0.063240   0.063300 -0.057458
        GMXUSDT          3.8  32.039895  32.403943  1.383385
        BAKEUSDT      -644.0   0.094700   0.095079 -0.244334

    """
    position_data = retry_wrapper(
        exchange.fapiPrivateV2GetPositionRisk,
        func_name='fapiPrivateV2GetPositionRisk',
        enable_retry=enable_retry,
    )

    df = pd.DataFrame(position_data)
    columns = {
        'positionAmt': '当前持仓量',
        'entryPrice': '持仓均价',
        'markPrice': '当前价格',
        'unRealizedProfit': '持仓收益',
    }

    df.rename(columns=columns, inplace=True)
    df = df.astype({
        '当前持仓量': float,
        '持仓均价': float,
        '当前价格': float,
        '持仓收益': float,
    })
    df = df[df['当前持仓量'] != 0]
    df.set_index('symbol', inplace=True)

    df = df[['当前持仓量', '持仓均价', '当前价格', '持仓收益']]

    return df


def fetch_candle_data(symbol, end_time, time_interval, limit, enable_retry=False):
    """
    获取从end_time结束，往前limit个time_interval的K线数据
    :param symbol:
    :param end_time:
    :param time_interval:
    :param limit:
    :param enable_retry:
    :return:

         candle_begin_time     open     high      low    close      volume    close_time  quote_volume  trade_num  taker_buy_base_asset_volume  taker_buy_quote_asset_volume  ignore   symbol
0  2023-09-03 09:45:00  0.11313  0.11318  0.11293  0.11305   2011015.0  1.693706e+12  2.274066e+05      970.0                    1049545.0                  1.186792e+05     0.0  XLMUSDT
1  2023-09-03 10:00:00  0.11305  0.11306  0.11294  0.11306   1329590.0  1.693707e+12  1.502509e+05      707.0                     580394.0                  6.559064e+04     0.0  XLMUSDT
2  2023-09-03 10:15:00  0.11306  0.11309  0.11300  0.11305    461133.0  1.693708e+12  5.212690e+04      457.0                     161930.0                  1.830464e+04     0.0  XLMUSDT
3  2023-09-03 10:30:00  0.11305  0.11327  0.11302  0.11325    917635.0  1.693709e+12  1.038205e+05      659.0                     485197.0                  5.490486e+04     0.0  XLMUSDT
4  2023-09-03 10:45:00  0.11325  0.11347  0.11322  0.11335    789092.0  1.693710e+12  8.943848e+04      567.0                     456493.0                  5.173859e+04     0.0  XLMUSDT
5  2023-09-03 11:00:00  0.11335  0.11360  0.11335  0.11351   1051137.0  1.693711e+12  1.193369e+05      743.0                     468735.0                  5.320951e+04     0.0  XLMUSDT
6  2023-09-03 11:15:00  0.11351  0.11364  0.11348  0.11362   1291667.0  1.693712e+12  1.466971e+05      753.0                     676269.0                  7.680858e+04     0.0  XLMUSDT
    """
    start_time_dt = end_time - pd.to_timedelta(time_interval) * limit
    params = {
        'symbol': symbol,
        'interval': time_interval,
        'limit': limit,
        'startTime': int(start_time_dt.timestamp() * 1000)
    }
    try:
        kline_data = retry_wrapper(
            exchange.fapiPublicGetKlines,
            params,
            func_name='fapiPublicGetKlines',
            enable_retry=enable_retry,
        )
    except Exception as e:
        print(traceback.format_exc())
        return pd.DataFrame()

    df = pd.DataFrame(kline_data).astype(float)
    columns = {
        0: 'candle_begin_time',
        1: 'open',
        2: 'high',
        3: 'low',
        4: 'close',
        5: 'volume',
        6: 'close_time',
        7: 'quote_volume',
        8: 'trade_num',
        9: 'taker_buy_base_asset_volume',
        10: 'taker_buy_quote_asset_volume',
        11: 'ignore',
    }
    df.rename(columns=columns, inplace=True)
    df['symbol'] = symbol
    df.sort_values(by=['candle_begin_time'], inplace=True)
    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def fetch_all_candle_data(symbol_list, run_time, time_interval, limit, enable_retry=False):
    symbol_candle_data = {}
    for symbol in tqdm(symbol_list):
        df = fetch_candle_data(symbol, run_time, time_interval, limit, enable_retry)
        if df.empty:
            continue

        # 转换时区
        utc_offset = int(time.localtime().tm_gmtoff / 60 / 60)
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + timedelta(hours=utc_offset)

        # 删除未走完的那根K线
        df = df[df['candle_begin_time'] < run_time]

        symbol_candle_data[symbol] = df

    return symbol_candle_data


def fetch_ticker_price(enable_retry=False):
    """
    获取最新价格，返回以 symbol 为索引的价格 Series
    :param enable_retry:
    :return:

        symbol
        TRXUSDT               0.077140
        BAKEUSDT              0.095200
        SOLUSDT              19.691000
        SPELLUSDT             0.000386
        RADUSDT               1.345000
        SSVUSDT              13.670000
        ARUSDT                4.158000
        TOMOUSDT              1.277600
        ARBUSDT               0.908200
        LRCUSDT               0.180700
        DOGEUSDT              0.063170
        NEOUSDT               7.108000

    """
    ticker_data = retry_wrapper(
        exchange.fapiPublicGetTickerPrice,
        func_name='fapiPublicGetTickerPrice',
        enable_retry=enable_retry,
    )
    tickers = pd.DataFrame(ticker_data).astype({'price': float, 'time': float})
    tickers.set_index('symbol', inplace=True)

    return tickers['price']


def load_market():
    """
    获取交易所交易对信息，返回以 symbol 为索引的 DataFrame

    minQuantity         最小下单精度，比如BTC，一次最少买入0.001个
    pricePrecision      币种的价格精度，比如BTC，价格是25933.2，不能是25933.199
    minNotional         最小下单金额, Binance一般最少买5U

    可以查询 https://www.binance.com/zh-CN/futures/trading-rules/perpetual

    :return:

                  onboardDate           status quoteAsset     contractType  pricePrecision  minQuantity  minNotional
symbol
XLMUSDT         1569398400000          TRADING       USDT        PERPETUAL               5            0        5.000
TLMBUSD         1654585200000         SETTLING       BUSD        PERPETUAL               5            0        5.000
0        5.000
ICXUSDT         1569398400000          TRADING       USDT        PERPETUAL               4            0        5.000
1        5.000
ETHUSDT_230929  1687660200000          TRADING       USDT  CURRENT_QUARTER               2            3        5.000
ETHUSDT_231229  1692360900000          TRADING       USDT     NEXT_QUARTER               2            3        5.000
0        5.000
UNIBUSD         1659596400000         SETTLING       BUSD        PERPETUAL               3
    """
    exchange_data = retry_wrapper(
        exchange.fapiPublicGetExchangeInfo,
        func_name='fapiPublicGetExchangeInfo',
    )

    symbol_dict_list = exchange_data['symbols']
    df_list = []
    for symbol_info in symbol_dict_list:
        symbol = symbol_info['symbol']
        df_data = {
            'symbol': symbol,
            'onboardDate': int(symbol_info['onboardDate']),
            'status': symbol_info['status'],
            'quoteAsset': symbol_info['quoteAsset'],
            'contractType': symbol_info['contractType'],
        }

        for _filter in symbol_info['filters']:
            if _filter['filterType'] == 'PRICE_FILTER':
                df_data['pricePrecision'] = int(math.log(float(_filter['tickSize']), 0.1))
            if _filter['filterType'] == 'LOT_SIZE':
                df_data['minQuantity'] = int(math.log(float(_filter['minQty']), 0.1))
            if _filter['filterType'] == 'MIN_NOTIONAL':
                df_data['minNotional'] = float(_filter['notional'])

        df_list.append(df_data)

    df = pd.DataFrame(df_list)
    df.set_index('symbol', inplace=True)

    return df


def place_order(symbol_order, symbol_market_info, enable_retry=False, enable_place_order=False):
    """
    根据最新的价格去交易所下单，主要针对最小下单量、价格精度还有下单金额进行处理

    结果返回每个订单的下单参数以及下单后交易所返回的结果
    :param symbol_order:
    :param symbol_market_info:
    :param enable_retry:
    :return:

下单参数:
    [
        {
            'symbol': 'FLMUSDT',
            'type': 'LIMIT',
            'timeInForce': 'GTC',
            'newClientOrderId': '1693789263.09107',
            'side': 'SELL',
            'price': '0.0569',
            'quantity': '1108.0',
            'reduceOnly': 'False'
        }, {
            'symbol': 'PHBUSDT',
            'type': 'LIMIT',
            'timeInForce': 'GTC',
            'newClientOrderId': '1693789263.0916202',
            'side': 'BUY',
            'price': '0.5904',
            'quantity': '110.0',
            'reduceOnly': 'True'
        }
    ]

批量下单结果:
    [
        {
            'orderId': '4443131794',
            'symbol': 'FLMUSDT',
            'status': 'NEW',
            'clientOrderId': '1693789263.09107',
            'price': '0.0569',
            'avgPrice': '0.00',
            'origQty': '1108',
            'executedQty': '0',
            'cumQty': '0',
            'cumQuote': '0.0000',
            'timeInForce': 'GTC',
            'type': 'LIMIT',
            'reduceOnly': False,
            'closePosition': False,
            'side': 'SELL',
            'positionSide': 'BOTH',
            'stopPrice': '0.0000',
            'workingType': 'CONTRACT_PRICE',
            'priceProtect': False,
            'origType': 'LIMIT',
            'updateTime': '1693789266659'
        }
    ]

    """
    order_params = []
    symbol_ticker_price = fetch_ticker_price(enable_retry)

    for symbol, row in symbol_order.iterrows():
        min_qty = symbol_market_info.at[symbol, 'minQuantity']
        min_notional = symbol_market_info.at[symbol, 'minNotional']
        price_precision = symbol_market_info.at[symbol, 'pricePrecision']

        if pd.isna(min_qty) or pd.isna(min_notional) or pd.isna(price_precision) :
            raise Exception('当前币种没有最小下单精度或者最小价格精度，币种信息异常')

        quantity = row['实际下单量']
        quantity = round(quantity, min_qty)
        # 根据订单的买卖方向以一定滑点确定下单价格
        if quantity > 0:
            side = 'BUY'
            price = symbol_ticker_price[symbol] * 1.015
        else:
            side = 'SELL'
            price = symbol_ticker_price[symbol] * 0.985
        quantity = abs(quantity)
        price = round(price, price_precision)
        reduce_only = (row['交易模式'] == '清仓')

        # 下单金额小于5块交易所不接受
        if quantity * price < min_notional:
            # 清仓允许比5U少的单
            if not reduce_only:
                print(symbol, '交易金额小于最小下单金额，跳过该笔交易')
                print('下单量: ', quantity, '价格: ', price, '最小下单金额: ', min_notional)
                continue

        params = dict()
        params['symbol'] = symbol
        params['type'] = 'LIMIT'
        params['timeInForce'] = 'GTC'
        params['newClientOrderId'] = str(time.time())
        params['side'] = side
        params['price'] = str(price)
        params['quantity'] = str(quantity)
        params['reduceOnly'] = str(reduce_only)
        order_params.append(params)

    print('每个币种的下单参数: ', order_params)
    order_results = []

    if not enable_place_order:
        return order_params, order_params

    for i in range(0, len(order_params), 5):
        order_params = order_params[i:i+5]
        try:
            result = retry_wrapper(
                exchange.fapiPrivatePostBatchOrders,
                params={'batchOrders': json.dumps(order_params)},
                func_name='fapiPrivatePostBatchOrders',
                enable_retry=enable_retry,
            )
            print('批量下单完成，批量下单结果: ', result)
            order_results += result
        except Exception as e:
            print(e)
            continue

    return order_params, order_results


