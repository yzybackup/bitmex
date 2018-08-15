# -*- coding: utf-8 -*- # 
from __future__ import absolute_import
from time import sleep
import sys
from datetime import datetime
from os.path import getmtime
import random
import requests
import atexit
import signal
import talib
import pandas as pd
import numpy as np
from market_maker import bitmex
from market_maker.settings import settings
from market_maker.utils import log, constants, errors, math

# Used for reloading the bot - saves modified times of key files
import os
watched_files_mtimes = [(f, getmtime(f)) for f in settings.WATCHED_FILES]


#
# Helpers
#
logger = log.setup_custom_logger('root')


class ExchangeInterface:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        if len(sys.argv) > 1:
            self.symbol = sys.argv[1]
        else:
            self.symbol = settings.SYMBOL
        self.bitmex = bitmex.BitMEX(base_url=settings.BASE_URL, symbol=self.symbol,
                                    apiKey=settings.API_KEY, apiSecret=settings.API_SECRET,
                                    orderIDPrefix=settings.ORDERID_PREFIX, postOnly=settings.POST_ONLY,
                                    timeout=settings.TIMEOUT)

    def cancel_order(self, order):
        tickLog = self.get_instrument()['tickLog']
        logger.info("Canceling: %s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
        while True:
            try:
                self.bitmex.cancel(order['orderID'])
                sleep(settings.API_REST_INTERVAL)
            except ValueError as e:
                logger.info(e)
                sleep(settings.API_ERROR_INTERVAL)
            else:
                break

    def cancel_all_orders(self):
        if self.dry_run:
            return

        logger.info("Resetting current position. Canceling all existing orders.")
        tickLog = self.get_instrument()['tickLog']

        # In certain cases, a WS update might not make it through before we call this.
        # For that reason, we grab via HTTP to ensure we grab them all.
        orders = self.bitmex.http_open_orders()

        for order in orders:
            logger.info("Canceling: %s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))

        if len(orders):
            self.bitmex.cancel([order['orderID'] for order in orders])

        sleep(settings.API_REST_INTERVAL)

    def get_portfolio(self):
        contracts = settings.CONTRACTS
        portfolio = {}
        for symbol in contracts:
            position = self.bitmex.position(symbol=symbol)
            instrument = self.bitmex.instrument(symbol=symbol)

            if instrument['isQuanto']:
                future_type = "Quanto"
            elif instrument['isInverse']:
                future_type = "Inverse"
            elif not instrument['isQuanto'] and not instrument['isInverse']:
                future_type = "Linear"
            else:
                raise NotImplementedError("Unknown future type; not quanto or inverse: %s" % instrument['symbol'])

            if instrument['underlyingToSettleMultiplier'] is None:
                multiplier = float(instrument['multiplier']) / float(instrument['quoteToSettleMultiplier'])
            else:
                multiplier = float(instrument['multiplier']) / float(instrument['underlyingToSettleMultiplier'])

            portfolio[symbol] = {
                "currentQty": float(position['currentQty']),
                "futureType": future_type,
                "multiplier": multiplier,
                "markPrice": float(instrument['markPrice']),
                "spot": float(instrument['indicativeSettlePrice'])
            }

        return portfolio

    def calc_delta(self):
        """Calculate currency delta for portfolio"""
        portfolio = self.get_portfolio()
        spot_delta = 0
        mark_delta = 0
        for symbol in portfolio:
            item = portfolio[symbol]
            if item['futureType'] == "Quanto":
                spot_delta += item['currentQty'] * item['multiplier'] * item['spot']
                mark_delta += item['currentQty'] * item['multiplier'] * item['markPrice']
            elif item['futureType'] == "Inverse":
                spot_delta += (item['multiplier'] / item['spot']) * item['currentQty']
                mark_delta += (item['multiplier'] / item['markPrice']) * item['currentQty']
            elif item['futureType'] == "Linear":
                spot_delta += item['multiplier'] * item['currentQty']
                mark_delta += item['multiplier'] * item['currentQty']
        basis_delta = mark_delta - spot_delta
        delta = {
            "spot": spot_delta,
            "mark_price": mark_delta,
            "basis": basis_delta
        }
        return delta

    def get_delta(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.get_position(symbol)['currentQty']

    def get_instrument(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.instrument(symbol)

    def get_margin(self):
        if self.dry_run:
            return {'marginBalance': float(settings.DRY_BTC), 'availableFunds': float(settings.DRY_BTC)}
        return self.bitmex.funds()

    def get_orders(self):
        if self.dry_run:
            return []
        return self.bitmex.open_orders()

    def get_highest_buy(self):
        buys = [o for o in self.get_orders() if o['side'] == 'Buy']
        if not len(buys):
            return {'price': -2**32}
        highest_buy = max(buys or [], key=lambda o: o['price'])
        return highest_buy if highest_buy else {'price': -2**32}

    def get_lowest_sell(self):
        sells = [o for o in self.get_orders() if o['side'] == 'Sell']
        if not len(sells):
            return {'price': 2**32}
        lowest_sell = min(sells or [], key=lambda o: o['price'])
        return lowest_sell if lowest_sell else {'price': 2**32}  # ought to be enough for anyone

    def get_position(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.position(symbol)
    
    def get_market_depth(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.market_depth(symbol)
    
    def get_market_depth_10(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.market_depth_10(symbol)
    
    def get_trade_bucket(self, binSize='5m', count=100, reverse=True):
        return self.bitmex.http_get_trade_bucket(binSize=binSize,
                                                 count=count,
                                                 reverse=reverse)
    
    def get_quote_5m(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.quote_5m(symbol)

    def get_trade_5m(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.trade_5m(symbol)

    def get_trade_1m(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.trade_1m(symbol)
    
    def get_trade_current(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.recent_trades()
    
    def calc_trade_side(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        trades = self.get_trade_current()
        sell_size = 0
        buy_size = 0
        sell_price = 0
        buy_price = 0
        for trade in trades:
            if trade.get('side') == 'Sell':
                sell_size += trade.get('size', 0)
                sell_price = trade.get('price', 0)
            elif trade.get('side') == 'Buy':
                buy_size += trade.get('size', 0)
                buy_price = trade.get('price', 0)
        logger.info('sell_size: %s, buy_size: %s, sell_price: %s, buy_price: %s' %
                    (sell_size, buy_size, sell_price, buy_price))
        return {
            'sell_size': sell_size,
            'buy_size': buy_size,
            'sell_price': sell_price,
            'buy_price': buy_price,
        }

    def get_quote_1h(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.quote_1h(symbol)

    def get_trade_1h(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.trade_1h(symbol) 

    def get_ticker(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        ticker = self.bitmex.ticker_data(symbol)
        self.current_price = ticker['mid']
        return ticker

    def is_open(self):
        """Check that websockets are still open."""
        return not self.bitmex.ws.exited

    def check_market_open(self):
        instrument = self.get_instrument()
        if instrument["state"] != "Open" and instrument["state"] != "Closed":
            raise errors.MarketClosedError("The instrument %s is not open. State: %s" %
                                           (self.symbol, instrument["state"]))

    def check_if_orderbook_empty(self):
        """This function checks whether the order book is empty"""
        instrument = self.get_instrument()
        if instrument['midPrice'] is None:
            raise errors.MarketEmptyError("Orderbook is empty, cannot quote")

    def amend_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.amend_bulk_orders(orders)

    def create_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.create_bulk_orders(orders)

    def cancel_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.cancel([order['orderID'] for order in orders])
    
    def calc_MACD(self, fastperiod=12, slowperiod=26, signalperiod=9):
        ohlcv_candles = self.candles_5m
        close_values = ohlcv_candles.close.values[::-1]
        macd, signal, hist = talib.MACD(close_values, 
                                        fastperiod = fastperiod, 
                                        slowperiod = slowperiod, 
                                        signalperiod = signalperiod)

        RSI = talib.RSI(close_values, timeperiod=fastperiod)
        MOM = talib.MOM(close_values, timeperiod=5)

        return {'macd': macd, 'signal': signal, 'hist': hist,
                'RSI': RSI, 'MOM': MOM}
    
    def combination_strategy(self, ):
        operator = 0
        try:
            self.candles_1m = pd.DataFrame(self.get_trade_bucket(binSize='1m'))
            self.candles_5m = pd.DataFrame(self.get_trade_bucket(binSize='5m'))
            self.candles_1h = pd.DataFrame(self.get_trade_bucket(binSize='1h'))
            self.candles_1d = pd.DataFrame(self.get_trade_bucket(binSize='1d'))
        except Exception as e:
            logger.exception(e)
            return 0
        
        operator += self.policy_MACD()
        
        #operator += self.policy_EMA()
        
        #operator += self.policy_BBANDS_short()
        
        operator += self.policy_BBANDS_long()
        
        operator += self.policy_GUPPY()
        
        if operator >= 10 and self.price_limit(1) > 0:
            return 1
        elif operator <= -10 and self.price_limit(-1) < 0:
            return -1
        return 0
    
    def volume_limit(self, volume):
        volume_values = np.array(volume[::-1], dtype='f8')
        volume_macd, volume_signal, volume_hist = \
                             talib.MACD(volume_values, 
                                        fastperiod = 12, 
                                        slowperiod = 26, 
                                        signalperiod = 9)
        volume_hist_1 = volume_hist[-1]
        volume_hist_2 = volume_hist[-2]
        logger.info('volume_hist_1: %s, volume_hist_2: %s' %
                    (volume_hist_1, volume_hist_2))
        if volume_hist_1 > 0:
            return True
        return False
    
    def price_limit(self, flags):
        '''
            近期高位不做多，低位不做空
        '''
        close_values_1h = self.candles_1h.close.values[::-1]
        EMA_PRICE = talib.EMA(close_values_1h, timeperiod=3)
        logger.info('the 1h ema_3 is: %s' % EMA_PRICE[-1])
        
        if flags < 0 and self.current_price > EMA_PRICE[-1] - 50:
            return -1
        if flags > 0 and self.current_price < EMA_PRICE[-1] + 50:
            return 1
        return 0

    def policy_GUPPY(self, ):
        '''
            顾比均线策略，选取3、5、8、10、12、15作为短期均线，
            30、35、40、45、50、60位长期均线。
            1. 1h看趋势，5m入场
            2. 1h短期均线在长期均线上方则为多头趋势，否则为空头趋势
            3. 5m如果短期穿过长期则为做多信号，要求短线展开
            4. 5m volume MACD，hist正数代表放量，可以入场
            5. 5m MACD的hist值处于正值为多头信号，反之空头
        '''
        logger.info('================begin GUPPY policy====================')
        close_values_1h = self.candles_1h.close.values[::-1]
        TREND_FAST_3 = talib.EMA(close_values_1h, timeperiod=3)
        TREND_FAST_5 = talib.EMA(close_values_1h, timeperiod=5)
        TREND_FAST_8 = talib.EMA(close_values_1h, timeperiod=8)
        TREND_FAST_10 = talib.EMA(close_values_1h, timeperiod=10)
        TREND_FAST_12 = talib.EMA(close_values_1h, timeperiod=12)
        TREND_FAST_15 = talib.EMA(close_values_1h, timeperiod=15)
        
        TREND_SLOW_30 = talib.EMA(close_values_1h, timeperiod=30)
        TREND_SLOW_35 = talib.EMA(close_values_1h, timeperiod=35)
        TREND_SLOW_40 = talib.EMA(close_values_1h, timeperiod=40)
        TREND_SLOW_45 = talib.EMA(close_values_1h, timeperiod=45)
        TREND_SLOW_50 = talib.EMA(close_values_1h, timeperiod=50)
        TREND_SLOW_60 = talib.EMA(close_values_1h, timeperiod=60)
        
        close_values_5m = self.candles_5m.close.values[::-1]
        macd_5m, signal_5m, hist_5m = talib.MACD(close_values_5m, 
                                        fastperiod = 12, 
                                        slowperiod = 26, 
                                        signalperiod = 9)
        
        volume_limit = self.volume_limit(self.candles_5m.volume.values)
        
        SIG_FAST_3 = talib.EMA(close_values_5m, timeperiod=3)
        SIG_FAST_5 = talib.EMA(close_values_5m, timeperiod=5)
        SIG_FAST_8 = talib.EMA(close_values_5m, timeperiod=8)
        SIG_FAST_10 = talib.EMA(close_values_5m, timeperiod=10)
        SIG_FAST_12 = talib.EMA(close_values_5m, timeperiod=12)
        SIG_FAST_15 = talib.EMA(close_values_5m, timeperiod=15)
        
        SIG_SLOW_30 = talib.EMA(close_values_5m, timeperiod=30)
        SIG_SLOW_35 = talib.EMA(close_values_5m, timeperiod=35)
        SIG_SLOW_40 = talib.EMA(close_values_5m, timeperiod=40)
        SIG_SLOW_45 = talib.EMA(close_values_5m, timeperiod=45)
        SIG_SLOW_50 = talib.EMA(close_values_5m, timeperiod=50)
        SIG_SLOW_60 = talib.EMA(close_values_5m, timeperiod=60)
        
        policy_data = {
            'TREND_FAST': [TREND_FAST_3[-1],
                           TREND_FAST_5[-1],
                           TREND_FAST_8[-1],
                           TREND_FAST_10[-1],
                           TREND_FAST_12[-1],
                           TREND_FAST_15[-1],
                           ],
            'TREND_SLOW': [TREND_SLOW_30[-1],
                           TREND_SLOW_35[-1],
                           TREND_SLOW_40[-1],
                           TREND_SLOW_45[-1],
                           TREND_SLOW_50[-1],
                           TREND_SLOW_60[-1],
                           ],
            
            'SIG_FAST': [SIG_FAST_3[-1],
                           SIG_FAST_5[-1],
                           SIG_FAST_8[-1],
                           SIG_FAST_10[-1],
                           SIG_FAST_12[-1],
                           SIG_FAST_15[-1],
                           ],
            'SIG_SLOW': [SIG_SLOW_30[-1],
                           SIG_SLOW_35[-1],
                           SIG_SLOW_40[-1],
                           SIG_SLOW_45[-1],
                           SIG_SLOW_50[-1],
                           SIG_SLOW_60[-1],
                           ],
            
            'SIG_FAST_PRE': [SIG_FAST_3[-2],
                           SIG_FAST_5[-2],
                           SIG_FAST_8[-2],
                           SIG_FAST_10[-2],
                           SIG_FAST_12[-2],
                           SIG_FAST_15[-2],
                           ],
            'SIG_SLOW_PRE': [SIG_SLOW_30[-2],
                           SIG_SLOW_35[-2],
                           SIG_SLOW_40[-2],
                           SIG_SLOW_45[-2],
                           SIG_SLOW_50[-2],
                           SIG_SLOW_60[-2],
                           ],
            
            'volume_limit': volume_limit,
            'hist_5m': hist_5m[-1],
            'trend': 0,
            'exchange_sig': 0,
            'operator': 0,
        }
        
        def list_com(fast_list, slow_list):
            '''
                如果fast_list的所有值大于slow_list里的所有值，返回1
                如果fast_list的所有值小于slow_list里的所有值，返回-1
                其他返回0
            '''
            buy_tmp = 0
            sell_tmp = 0
            for fast in fast_list:
                for slow in slow_list:
                    if fast > slow:
                        buy_tmp += 1
                    else:
                        sell_tmp += 1
            
            if buy_tmp == 36:
                return 1
            if sell_tmp == 36:
                return -1
            return 0
        
        def near_com(flags):
            cross_sig = 0
            pass_sig = 0
            for i in range(-1, -21, -1):
                slow_list = [
                    SIG_SLOW_30[i],
                    SIG_SLOW_35[i],
                    SIG_SLOW_40[i],
                    SIG_SLOW_45[i],
                    SIG_SLOW_50[i],
                    SIG_SLOW_60[i],
                ]
                fast_list = [
                    SIG_FAST_3[i],
                    SIG_FAST_5[i],
                    SIG_FAST_8[i],
                    SIG_FAST_10[i],
                    SIG_FAST_12[i],
                    SIG_FAST_15[i],
                ]

                if pass_sig==0 and list_com(fast_list, slow_list) == flags:
                    pass_sig = abs(i)
                    logger.info('pass_sig=%s:fast_list:%s,slow_list:%s' % (i, fast_list, slow_list))
                if cross_sig==0 and list_com(fast_list, slow_list) == -flags:
                    cross_sig = abs(i)
                    logger.info('cross_sig=%s:fast_list:%s,slow_list:%s' % (i, fast_list, slow_list))
            
            if pass_sig and cross_sig and pass_sig < cross_sig:
                return flags

            return 0

        logger.info('policy_data: %s' % policy_data)

        if list_com(policy_data['TREND_FAST'], policy_data['TREND_SLOW']) > 0:
            policy_data['trend'] = 1
        elif list_com(policy_data['TREND_FAST'], policy_data['TREND_SLOW']) < 0:
            policy_data['trend'] = -1

        if policy_data['trend'] > 0 and volume_limit and \
           policy_data['hist_5m'] > 2 and near_com(1) == 1:
            # 做多信号
            policy_data['exchange_sig'] = 1

        if policy_data['trend'] < 0 and volume_limit and \
           policy_data['hist_5m'] < -2 and near_com(-1) == -1:
            # 做空信号
            policy_data['exchange_sig'] = -1
        
        if policy_data['exchange_sig'] > 0:
            logger.info('=========Buy Opportunity!=========')
            policy_data['operator'] += 10
        elif policy_data['exchange_sig'] < 0:
            policy_data['operator'] -= 10
            logger.info('=========Sell Opportunity!=========')
        else:
            logger.info('=========No Opportunity!=========')
        logger.info('policy_data: %s' % policy_data)
        logger.info('================end GUPPY policy====================')
        return policy_data['operator']

    
    def policy_BBANDS_long(self, ):
        '''
            布林线，MACD，volume MACD混合策略（由于指标迟滞性，改为做反弹） 
            1. 1h布林线，布林宽度BB width，宽度大于0.03时才入场，避免横盘（上轨-下轨）/中轨
            2. 布林%B指标>1为多，小于<0为空。（收盘-下轨）/（上轨-下轨）
            3. 1h MACD的hist值处于正值为多头趋势，反之空头
            4. volume MACD，hist正数代表放量，可以入场
            5. 所有指标使用1h线，100条
            6. MACD参数为12，26，9
        '''
        logger.info('================begin BBANDS_long policy====================')
        volume_values_1h = np.array(self.candles_1h.volume.values[::-1], dtype='f8')
        volume_macd_1h, volume_signal_1h, volume_hist_1h = \
                             talib.MACD(volume_values_1h, 
                                        fastperiod = 12, 
                                        slowperiod = 26, 
                                        signalperiod = 9)
        close_values_1h = self.candles_1h.close.values[::-1]
        upper, middle, lower = \
            talib.BBANDS(close_values_1h,
                         timeperiod=20,
                         # number of non-biased standard deviations from the mean
                         nbdevup=2,
                         nbdevdn=2,
                         # Moving average type: simple moving average here
                         matype=0)
        macd_1h, signal_1h, hist_1h = talib.MACD(close_values_1h, 
                                        fastperiod = 12, 
                                        slowperiod = 26, 
                                        signalperiod = 9)
        ticker = self.get_ticker()
        
        policy_data = {
            'volume_hist_1h': volume_hist_1h[-1],
            'hist_1h': hist_1h[-1],
            'upper': upper[-1],
            'middle': middle[-1],
            'lower': lower[-1],
            'boll_w': (upper[-1] - lower[-1])/middle[-1],
            'boll_b':  (close_values_1h[-1] - lower[-1])/(upper[-1] - lower[-1]),  
            'exchange_sig': 0,
            'operator': 0,
            'price': ticker['mid']
        }
        if policy_data['volume_hist_1h'] > 0 and policy_data['hist_1h'] > 0 and \
           policy_data['boll_w'] > 0.03 and policy_data['boll_b'] > 1.3:
            # 做空
            policy_data['exchange_sig'] = -1
        if policy_data['volume_hist_1h'] > 0 and policy_data['hist_1h'] < 0 and \
           policy_data['boll_w'] > 0.03 and policy_data['boll_b'] < -0.3:
            # 做多
            policy_data['exchange_sig'] = 1

        if policy_data['exchange_sig'] > 0:
            logger.info('=========Buy Opportunity!=========')
            policy_data['operator'] += 10
        elif policy_data['exchange_sig'] < 0:
            policy_data['operator'] -= 10
            logger.info('=========Sell Opportunity!=========')
        else:
            logger.info('=========No Opportunity!=========')
        logger.info('policy_data: %s' % policy_data)
        logger.info('================end BBANDS_long policy====================')
        return policy_data['operator']
    
    
    def policy_BBANDS_short(self, ):
        '''
            布林线短线策略
            1. 5m BBAND 参数20，2，5m MACD 12，26，9
            2. 价格处于下轨附近，并且开口较大（宽度大于0.01），MACD出现金叉，且第二根hist值大于3，则做多
            3. 反之做空
            4. 5m放量才入场
        '''
        logger.info('================begin BBANDS_short policy====================')
        
        volume_limit = self.volume_limit(self.candles_1m.volume.values)
        
        close_values_5m = self.candles_1m.close.values[::-1]
        upper, middle, lower = \
            talib.BBANDS(close_values_5m,
                         timeperiod=20,
                         # number of non-biased standard deviations from the mean
                         nbdevup=2,
                         nbdevdn=2,
                         # Moving average type: simple moving average here
                         matype=0)
        macd_5m, signal_5m, hist_5m = talib.MACD(close_values_5m, 
                                        fastperiod = 12, 
                                        slowperiod = 26, 
                                        signalperiod = 9)
        ticker = self.get_ticker()
        policy_data = {
            'upper': upper[-1],
            'middle': middle[-1],
            'lower': lower[-1],
            'hist_5m_5': hist_5m[-5],
            'hist_5m_4': hist_5m[-4],
            'hist_5m_3': hist_5m[-3],
            'hist_5m_2': hist_5m[-2],
            'hist_5m_1': hist_5m[-1],
            'volume_limit': volume_limit,
            'boll_w': (upper[-1] - lower[-1])/middle[-1],
            'boll_b':  (close_values_5m[-1] - lower[-1])/(upper[-1] - lower[-1]),  
            'exchange_sig': 0,
            'operator': 0,
            'price': ticker['mid']
        }
        if policy_data['hist_5m_2'] < 0 and policy_data['hist_5m_1'] < 0 and \
           policy_data['hist_5m_5'] > policy_data['hist_5m_4'] and \
           policy_data['hist_5m_4'] > policy_data['hist_5m_3'] and \
           policy_data['hist_5m_3'] > policy_data['hist_5m_2'] and \
           policy_data['hist_5m_2'] > policy_data['hist_5m_1'] and \
           abs(policy_data['hist_5m_1']) > 1 and policy_data['hist_5m_3'] > 0 and\
           policy_data['boll_w'] > 0.005 and volume_limit:
            # 做空
            policy_data['exchange_sig'] = -1
        if policy_data['hist_5m_2'] > 0 and policy_data['hist_5m_1'] > 0 and \
           policy_data['hist_5m_5'] < policy_data['hist_5m_4'] and \
           policy_data['hist_5m_4'] < policy_data['hist_5m_3'] and \
           policy_data['hist_5m_3'] < policy_data['hist_5m_2'] and \
           policy_data['hist_5m_2'] < policy_data['hist_5m_1'] and \
           abs(policy_data['hist_5m_1']) > 1 and policy_data['hist_5m_3'] < 0 and\
           policy_data['boll_w'] > 0.005 and volume_limit:
            # 做多
            policy_data['exchange_sig'] = 1
    
        if policy_data['exchange_sig'] > 0:
            logger.info('=========Buy Opportunity!=========')
            policy_data['operator'] += 10
        elif policy_data['exchange_sig'] < 0:
            policy_data['operator'] -= 10
            logger.info('=========Sell Opportunity!=========')
        else:
            logger.info('=========No Opportunity!=========')
        
        logger.info('policy_data: %s' % policy_data)  
        logger.info('================end BBANDS_short policy====================')
        return policy_data['operator']
    
    def policy_MACD(self):
        '''
            1. 1d MACD看长线趋势，hist连着3次上涨为多头趋势，否则为空头
            2. 1h MACD看短线趋势，hist连着5次上涨为多头趋势，否则为空头
            3. 5m MACD为入场信号，macd近期出现（3根柱子以内）金叉为做多信号，
               且hist连着3次上涨，且当前hist大于1，否则做空。
            4. 金叉标准：macd上穿signal，且hist为正。
            5. 死叉标准：macd下穿signal，且hist为负。
            6. 5m放量入场
        '''

        logger.info('================begin MACD Comp policy====================')
        close_values_1d = self.candles_1d.close.values[::-1]
        macd_1d, signal_1d, hist_1d = talib.MACD(close_values_1d, 
                                        fastperiod = 12, 
                                        slowperiod = 26, 
                                        signalperiod = 9)
        close_values_1h = self.candles_1h.close.values[::-1]
        macd_1h, signal_1h, hist_1h = talib.MACD(close_values_1h, 
                                        fastperiod = 12, 
                                        slowperiod = 26, 
                                        signalperiod = 9)
        close_values_5m = self.candles_5m.close.values[::-1]
        macd_5m, signal_5m, hist_5m = talib.MACD(close_values_5m, 
                                        fastperiod = 12, 
                                        slowperiod = 26, 
                                        signalperiod = 9)
        
        volume_limit = self.volume_limit(self.candles_5m.volume.values)
    
        policy_data = {
            'hist_1d_3': hist_1d[-3],
            'hist_1d_2': hist_1d[-2],
            'hist_1d_1': hist_1d[-1],
            'hist_1h_5': hist_1h[-5],
            'hist_1h_4': hist_1h[-4],
            'hist_1h_3': hist_1h[-3],
            'hist_1h_2': hist_1h[-2],
            'hist_1h_1': hist_1h[-1],
            'macd_5m_4': macd_5m[-4],
            'macd_5m_3': macd_5m[-3],
            'macd_5m_2': macd_5m[-2],
            'macd_5m_1': macd_5m[-1],
            'signal_5m_4': signal_5m[-4],
            'signal_5m_3': signal_5m[-3],
            'signal_5m_2': signal_5m[-2],
            'signal_5m_1': signal_5m[-1],
            'hist_5m_4': hist_5m[-4],
            'hist_5m_3': hist_5m[-3],
            'hist_5m_2': hist_5m[-2],
            'hist_5m_1': hist_5m[-1],
            'volume_limit': volume_limit,
            'long_trend': 0,
            'short_trend': 0,
            'exchange_sig': 0,
            'operator': 0
        }
        
        # 长线趋势判断
        if policy_data['hist_1d_3'] > policy_data['hist_1d_2'] and \
           policy_data['hist_1d_2'] > policy_data['hist_1d_1']:
            # 长线空头趋势
            policy_data['long_trend'] = -1
        if policy_data['hist_1d_3'] < policy_data['hist_1d_2'] and \
           policy_data['hist_1d_2'] < policy_data['hist_1d_1']:
            # 长线多头趋势
            policy_data['long_trend'] = 1
        if policy_data['hist_1h_3'] > policy_data['hist_1h_2'] and \
           policy_data['hist_1h_2'] > policy_data['hist_1h_1']:
            # 短线空头趋势
            policy_data['short_trend'] = -1
        if policy_data['hist_1h_3'] < policy_data['hist_1h_2'] and \
           policy_data['hist_1h_2'] < policy_data['hist_1h_1']:
            # 短线多头趋势
            policy_data['short_trend'] = 1
        
        if policy_data['hist_5m_4'] > 0 and \
           policy_data['hist_5m_1'] < 0 and \
           policy_data['hist_5m_3'] > policy_data['hist_5m_2'] and \
           policy_data['hist_5m_2'] > policy_data['hist_5m_1'] and \
           policy_data['hist_5m_1'] < -1 and policy_data['short_trend'] < 0 and \
           policy_data['long_trend'] < 0 and volume_limit:
            # 空头信号
            policy_data['exchange_sig'] = -1
        if policy_data['hist_5m_4'] < 0 and \
           policy_data['hist_5m_1'] > 0 and \
           policy_data['hist_5m_3'] < policy_data['hist_5m_2'] and \
           policy_data['hist_5m_2'] < policy_data['hist_5m_1'] and \
           policy_data['hist_5m_1'] > 1 and policy_data['short_trend'] > 0 and \
           policy_data['long_trend'] > 0 and volume_limit:
            # 多头信号
            policy_data['exchange_sig'] = 1
        
        if policy_data['exchange_sig'] > 0:
            logger.info('=========Buy Opportunity!=========')
            policy_data['operator'] += 10
        elif policy_data['exchange_sig'] < 0:
            policy_data['operator'] -= 10
            logger.info('=========Sell Opportunity!=========')
        else:
            logger.info('=========No Opportunity!=========')

        logger.info('policy_data: %s' % policy_data)  
        logger.info('================end MACD Comp policy====================')
        return policy_data['operator']
    
    def policy_EMA(self):
        '''
            1. H1周期的EMA5与EMA80作为趋势判断，EMA5大于EMA80为做多趋势，EMA小于EMA80为做空趋势
            2. M5周期EMA5上穿EMA80为做多信号，需与1趋势相同；
            3. M5周期EMA5下穿EMA80为做空信号，需与1趋势相同；
        '''
        close_values_1h = self.candles_1h.close.values[::-1]
        EMA_FAST_1h = talib.EMA(close_values_1h, timeperiod=5)
        EMA_SLOW_1h = talib.EMA(close_values_1h, timeperiod=80)
        
        close_values_5m = self.candles_5m.close.values[::-1]
        EMA_FAST_5m = talib.EMA(close_values_5m, timeperiod=5)
        EMA_SLOW_5m = talib.EMA(close_values_5m, timeperiod=80)

        logger.info('================begin EMA policy====================')
        logger.info('EMA_FAST_1h[-1]: %s, EMA_SLOW_1h[-1]: %s,'
                    'EMA_FAST_5m[-2]: %s, EMA_SLOW_5m[-2]: %s,'
                    'EMA_FAST_5m[-1]: %s, EMA_SLOW_5m[-1]: %s' %
                    (EMA_FAST_1h[-1], EMA_SLOW_1h[-1], EMA_FAST_5m[-2],
                     EMA_SLOW_5m[-2], EMA_FAST_5m[-1], EMA_SLOW_5m[-1]))
        if EMA_FAST_1h[-1] > EMA_SLOW_1h[-1] and \
           EMA_FAST_5m[-2] < EMA_SLOW_5m[-2] and \
           EMA_FAST_5m[-1] > EMA_SLOW_5m[-1]:
            # buy
            logger.info('=========Buy Opportunity!=========')
            return 10
        if EMA_FAST_1h[-1] < EMA_SLOW_1h[-1] and \
           EMA_FAST_5m[-2] > EMA_SLOW_5m[-2] and \
           EMA_FAST_5m[-1] < EMA_SLOW_5m[-1]:
            # sell
            logger.info('=========Sell Opportunity!=========')
            return -10
        logger.info('=========No Opportunity!=========')
        logger.info('================end EMA policy====================')
        return 0


class OrderManager:
    def __init__(self):
        self.exchange = ExchangeInterface(settings.DRY_RUN)
        # Once exchange is created, register exit handler that will always cancel orders
        # on any error.
        atexit.register(self.exit)
        signal.signal(signal.SIGTERM, self.exit)

        logger.info("Using symbol %s." % self.exchange.symbol)

        if settings.DRY_RUN:
            logger.info("Initializing dry run. Orders printed below represent what would be posted to BitMEX.")
        else:
            logger.info("Order Manager initializing, connecting to BitMEX. Live run: executing real trades.")

        self.start_time = datetime.now()
        self.instrument = self.exchange.get_instrument()
        self.starting_qty = self.exchange.get_delta()
        self.running_qty = self.starting_qty
        #self.reset()

    def reset(self):
        self.exchange.cancel_all_orders()
        self.sanity_check()
        self.print_status()

        # Create orders and converge.
        self.place_orders()

    def print_status(self):
        """Print the current MM status."""

        margin = self.exchange.get_margin()
        position = self.exchange.get_position()
        self.running_qty = self.exchange.get_delta()
        tickLog = self.exchange.get_instrument()['tickLog']
        self.start_XBt = margin["marginBalance"]
        
        logger.info("Current XBT Balance: %.6f" % XBt_to_XBT(self.start_XBt))
        logger.info("Current Contract Position: %d" % self.running_qty)
        if settings.CHECK_POSITION_LIMITS:
            logger.info("Position limits: %d/%d" % (settings.MIN_POSITION, settings.MAX_POSITION))
        if position['currentQty'] != 0:
            logger.info("Avg Cost Price: %.*f" % (tickLog, float(position['avgCostPrice'])))
            logger.info("Avg Entry Price: %.*f" % (tickLog, float(position['avgEntryPrice'])))
        logger.info("Contracts Traded This Run: %d" % (self.running_qty - self.starting_qty))
        logger.info("Total Contract Delta: %.4f XBT" % self.exchange.calc_delta()['spot'])
        logger.info("==============================================")
        logger.info("Current Depth: %s" % self.exchange.get_market_depth_10())
        #logger.info("Current trade_current: %s" % self.exchange.get_trade_current())
        #logger.info("Current trade_1m: %s" % self.exchange.get_trade_1m())
        #logger.info("Current trade_5m: %s" % self.exchange.get_trade_5m())
        #logger.info("Current quote_5m: %s" % self.exchange.get_quote_5m())
        #logger.info("Current trade_1h: %s" % self.exchange.get_trade_1h())
        #logger.info("Current quote_1h: %s" % self.exchange.get_quote_1h())
        logger.info("==============================================")
        logger.info("Current get_position: %s" % self.exchange.get_position())
        #logger.info("Current get_trade_bucket: %s" % self.exchange.get_trade_bucket())
        #logger.info("Current calc_MACD: %s" % self.exchange.calc_MACD())

    def get_ticker(self):
        ticker = self.exchange.get_ticker()
        tickLog = self.exchange.get_instrument()['tickLog']

        # Set up our buy & sell positions as the smallest possible unit above and below the current spread
        # and we'll work out from there. That way we always have the best price but we don't kill wide
        # and potentially profitable spreads.
        self.start_position_buy = ticker["buy"] + self.instrument['tickSize']
        self.start_position_sell = ticker["sell"] - self.instrument['tickSize']

        # If we're maintaining spreads and we already have orders in place,
        # make sure they're not ours. If they are, we need to adjust, otherwise we'll
        # just work the orders inward until they collide.
        if settings.MAINTAIN_SPREADS:
            if ticker['buy'] == self.exchange.get_highest_buy()['price']:
                self.start_position_buy = ticker["buy"]
            if ticker['sell'] == self.exchange.get_lowest_sell()['price']:
                self.start_position_sell = ticker["sell"]

        # Back off if our spread is too small.
        if self.start_position_buy * (1.00 + settings.MIN_SPREAD) > self.start_position_sell:
            self.start_position_buy *= (1.00 - (settings.MIN_SPREAD / 2))
            self.start_position_sell *= (1.00 + (settings.MIN_SPREAD / 2))

        # Midpoint, used for simpler order placement.
        self.start_position_mid = ticker["mid"]
        logger.info(
            "%s Ticker: Buy: %.*f, Sell: %.*f" %
            (self.instrument['symbol'], tickLog, ticker["buy"], tickLog, ticker["sell"])
        )
        logger.info('Start Positions: Buy: %.*f, Sell: %.*f, Mid: %.*f' %
                    (tickLog, self.start_position_buy, tickLog, self.start_position_sell,
                     tickLog, self.start_position_mid))
        return ticker

    def get_price_offset(self, index):
        """Given an index (1, -1, 2, -2, etc.) return the price for that side of the book.
           Negative is a buy, positive is a sell."""
        # Maintain existing spreads for max profit
        if settings.MAINTAIN_SPREADS:
            start_position = self.start_position_buy if index < 0 else self.start_position_sell
            # First positions (index 1, -1) should start right at start_position, others should branch from there
            index = index + 1 if index < 0 else index - 1
        else:
            # Offset mode: ticker comes from a reference exchange and we define an offset.
            start_position = self.start_position_buy if index < 0 else self.start_position_sell

            # If we're attempting to sell, but our sell price is actually lower than the buy,
            # move over to the sell side.
            if index > 0 and start_position < self.start_position_buy:
                start_position = self.start_position_sell
            # Same for buys.
            if index < 0 and start_position > self.start_position_sell:
                start_position = self.start_position_buy

        return math.toNearest(start_position * (1 + settings.INTERVAL) ** index, self.instrument['tickSize'])

    ###
    # Orders
    ###

    def place_orders(self):
        """Create order items for use in convergence."""

        buy_orders = []
        sell_orders = []
        # Create orders from the outside in. This is intentional - let's say the inner order gets taken;
        # then we match orders from the outside in, ensuring the fewest number of orders are amended and only
        # a new order is created in the inside. If we did it inside-out, all orders would be amended
        # down and a new order would be created at the outside.
        #for i in reversed(range(1, settings.ORDER_PAIRS + 1)):
        #    if not self.long_position_limit_exceeded():
        #        buy_orders.append(self.prepare_order(-i))
        #    if not self.short_position_limit_exceeded():
        #        sell_orders.append(self.prepare_order(i))
        
        current_depth = self.exchange.get_market_depth_10()
        #trade_5m = self.exchange.get_trade_5m()
        #trade_1h = self.exchange.get_trade_1h()
        #quote_5m = self.exchange.get_quote_5m()
        #quote_1h = self.exchange.get_quote_1h()
        portfolio = self.exchange.get_portfolio()
        logger.info('portfolio: %s' % portfolio)
        current_trade_info = self.exchange.calc_trade_side()

        bids_num = 0
        asks_num = 0
        bid_ask_sig = ''

        if current_depth and portfolio:
            bids = current_depth[0].get('bids')
            asks = current_depth[0].get('asks')
            for bid in bids:
                bids_num += bid[1]
            for ask in asks:
                asks_num += ask[1]
        
            if bids_num < asks_num and bids[0][1]*10 < asks[0][1] and\
               portfolio['XBTUSD'].get('spot')+10 < self.start_position_mid and\
               current_trade_info['sell_size'] > 10*current_trade_info['buy_size']:
                bid_ask_sig = 'sell'
            if bids_num > asks_num and bids[0][1] > asks[0][1]*10 and \
               portfolio['XBTUSD'].get('spot')-10 > self.start_position_mid and\
                current_trade_info['buy_size'] > 10*current_trade_info['sell_size']:
                bid_ask_sig = 'buy'
            logger.info('bids_num: %s, asks_num: %s, bid_one: %s, '
                        'ask_one: %s, markPrice: %s, lastPrice: %s, '
                        'bid_ask_sig: %s' %
                        (bids_num, asks_num, bids[0][1], asks[0][1],
                         portfolio['XBTUSD'].get('markPrice'),
                         self.start_position_mid, bid_ask_sig))
        else:
            return
        bid_ask_sig = ''
        combination_strategy = self.exchange.combination_strategy()
        if combination_strategy > 0:
            bid_ask_sig = 'buy'
        elif combination_strategy < 0:
            bid_ask_sig = 'sell'
        orders = []
        if bid_ask_sig == 'buy':
            if not self.long_position_limit_exceeded():
                orders = self.market_order(1)
        if bid_ask_sig == 'sell':
            if not self.short_position_limit_exceeded():
                orders = self.market_order(-1)
        return self.process_orders(orders)
    
    def process_orders(self, orders):
        position = self.exchange.get_position()
        if not position:
            return
        logger.info('orders: %s' % orders)
        if position.get('isOpen'):
            position_price = position.get('avgEntryPrice')
            liquidation_price = position.get('liquidationPrice')
            quantity = position.get('currentQty')
            open_side = ''
            if position_price > liquidation_price:
                open_side = 'buy'
            else:
                open_side = 'sell'
            self.update_stop_limit_order(position_price)
        elif orders:
            limit_order = []
            for i in range(len(orders)):
                if orders[i]['ordType'] == 'Limit':
                    limit_order.append(orders.pop(i))
            
            self.exchange.create_bulk_orders(orders)
            sleep(15)
            if limit_order:
                self.exchange.create_bulk_orders(limit_order)
            logger.info('========Order exchange Successful!======')
        
    def amend_stop_limit_order(self, position_price, open_side=None,
                               quantity=None ):
        exist_orders = self.exchange.get_orders()
        if len(exist_orders)<2:
            if position_price and open_side:
                order_limit = []
                order_stop = []
                if open_side == 'buy':
                    order_stop.append({'stopPx': self.start_position_mid-settings.ORDER_STOP_POINT,
                            'orderQty': quantity,
                            'ordType': 'Stop',
                            'execInst': 'Close,LastPrice',
                            'side': 'Sell'})
                    order_limit.append({'price': self.start_position_mid+settings.ORDER_LIMIT_POINT,
                            'orderQty': quantity,
                            'ordType': 'Limit',
                            'execInst': 'Close',
                            'side': 'Sell'})
                if open_side == 'sell':
                    order_stop.append({'stopPx': self.start_position_mid+settings.ORDER_STOP_POINT,
                            'orderQty': quantity,
                            'ordType': 'Stop',
                            'execInst': 'Close,LastPrice',
                            'side': 'Buy'})
                    order_limit.append({'price': self.start_position_mid-settings.ORDER_LIMIT_POINT,
                            'orderQty': quantity,
                            'ordType': 'Limit',
                            'execInst': 'Close',
                            'side': 'Buy'})
                if order_limit:
                    self.exchange.create_bulk_orders(order_limit)
                    sleep(3)
                if order_stop:
                    self.exchange.create_bulk_orders(order_stop)
                    sleep(3)
                logger.info('========Order update!===order_stop:%s, order_limit%s===' %
                            (order_stop, order_limit))
    

    def update_stop_limit_order(self, position_price, open_side=None,
                                quantity=None):
        exist_orders = self.exchange.get_orders()
        new_limit_price = 0
        new_stop_price = 0
        update_orders = []
        logger.info('update_stop_limit_order, exist_orders: %s' % exist_orders)
        for order in exist_orders:
            # kong dan zhi ying
            if order.get('ordType') == 'Limit' and order.get('side') == 'Buy':
                sell_limit = self.start_position_mid - settings.ORDER_LIMIT_STEP
                if sell_limit < order.get('price'):
                    update_orders.append({'orderID': order.get('orderID'),
                                          'price': sell_limit-10})
            # kong dan zhi sun
            if order.get('ordType') == 'Stop' and order.get('side') == 'Buy':
                move_con = position_price - self.start_position_mid
                if move_con >= settings.ORDER_MOVE_CONDITION and \
                   order.get('stopPx') - self.start_position_mid > settings.ORDER_STOP_STEP:
                    new_stop_price = self.start_position_mid + settings.ORDER_STOP_STEP
                    update_orders.append({'orderID': order.get('orderID'),
                                          'stopPx': new_stop_price}) 
            # duo dan zhi ying
            if order.get('ordType') == 'Limit' and order.get('side') == 'Sell':
                buy_limit = self.start_position_mid + settings.ORDER_LIMIT_STEP
                if buy_limit > order.get('price'):
                    update_orders.append({'orderID': order.get('orderID'),
                                          'price': buy_limit+10})
            # duo dan zhi sun
            if order.get('ordType') == 'Stop' and order.get('side') == 'Sell':
                move_con = self.start_position_mid - position_price
                if move_con >= settings.ORDER_MOVE_CONDITION and \
                   self.start_position_mid - order.get('stopPx') > settings.ORDER_STOP_STEP:
                    new_stop_price = self.start_position_mid - settings.ORDER_STOP_STEP
                    update_orders.append({'orderID': order.get('orderID'),
                                          'stopPx': new_stop_price})                    
        if update_orders:
            self.exchange.amend_bulk_orders(update_orders)
            logger.info('Update orders: %s' % update_orders)
    
    def market_order(self, index):
        quantity = settings.ORDER_START_SIZE
        orders = []
        if index > 0:
            '''
            Buy orders include market and stop orders.
            [
                {
                    "orderQty": 100,
                    "ordType": "Market",
                    "side": "Buy",
                    "symbol": "XBTUSD"},
                {
                    "stopPx": 7369.0,
                    "orderQty": 100,
                    "ordType": "Stop",
                    "side": "Sell",
                    "symbol": "XBTUSD"}
                ]
            Limit orders, this must wait for the market order to be created
            before you can create the limit order.
            [
                {
                    "price": 7299.0,
                    "orderQty": 100,
                    "ordType": "Limit",
                    "side": "Sell",
                    "symbol": "XBTUSD",
                    "execInst": "Close"}
                ]
            '''
            orders.append({'orderQty': quantity,
                           'ordType': 'Market',
                           'side': 'Buy'})
            orders.append({'stopPx': self.start_position_mid-settings.ORDER_STOP_POINT,
                            'orderQty': quantity,
                            'ordType': 'Stop',
                            'execInst': 'Close,LastPrice',
                            'side': 'Sell'})
            orders.append({'price': self.start_position_mid+settings.ORDER_LIMIT_POINT,
                            'orderQty': quantity,
                            'ordType': 'Limit',
                            'execInst': 'Close',
                            'side': 'Sell'})
        else:
            '''
            Sell orders include market and stop orders.
            [
                {
                    "orderQty": 100,
                    "ordType": "Market",
                    "side": "Sell",
                    "symbol": "XBTUSD"},
                {
                    "stopPx": 7369.0,
                    "orderQty": 100,
                    "ordType": "Stop",
                    "side": "Buy",
                    "symbol": "XBTUSD"}
                ]
            Limit orders, this must wait for the market order to be created
            before you can create the limit order.
            [
                {
                    "price": 7299.0,
                    "orderQty": 100,
                    "ordType": "Limit",
                    "side": "Buy",
                    "symbol": "XBTUSD",
                    "execInst": "Close"}
                ]
            '''
            orders.append({'orderQty': quantity,
                           'ordType': 'Market',
                           'side': 'Sell'})
            orders.append({'stopPx': self.start_position_mid+settings.ORDER_STOP_POINT,
                            'orderQty': quantity,
                            'ordType': 'Stop',
                            'execInst': 'Close,LastPrice',
                            'side': 'Buy'})
            orders.append({'price': self.start_position_mid-settings.ORDER_LIMIT_POINT,
                            'orderQty': quantity,
                            'ordType': 'Limit',
                            'execInst': 'Close',
                            'side': 'Buy'})
        return orders


    def prepare_order(self, index):
        """Create an order object."""

        if settings.RANDOM_ORDER_SIZE is True:
            quantity = random.randint(settings.MIN_ORDER_SIZE, settings.MAX_ORDER_SIZE)
        else:
            quantity = settings.ORDER_START_SIZE + ((abs(index) - 1) * settings.ORDER_STEP_SIZE)

        price = self.get_price_offset(index)

        return {'price': price, 'orderQty': quantity, 'side': "Buy" if index < 0 else "Sell"}

    def converge_orders(self, buy_orders, sell_orders):
        """Converge the orders we currently have in the book with what we want to be in the book.
           This involves amending any open orders and creating new ones if any have filled completely.
           We start from the closest orders outward."""

        tickLog = self.exchange.get_instrument()['tickLog']
        to_amend = []
        to_create = []
        to_cancel = []
        buys_matched = 0
        sells_matched = 0
        existing_orders = self.exchange.get_orders()

        # Check all existing orders and match them up with what we want to place.
        # If there's an open one, we might be able to amend it to fit what we want.
        for order in existing_orders:
            try:
                if order['side'] == 'Buy':
                    desired_order = buy_orders[buys_matched]
                    buys_matched += 1
                else:
                    desired_order = sell_orders[sells_matched]
                    sells_matched += 1

                # Found an existing order. Do we need to amend it?
                if desired_order['orderQty'] != order['leavesQty'] or (
                        # If price has changed, and the change is more than our RELIST_INTERVAL, amend.
                        desired_order['price'] != order['price'] and
                        abs((desired_order['price'] / order['price']) - 1) > settings.RELIST_INTERVAL):
                    to_amend.append({'orderID': order['orderID'], 'orderQty': order['cumQty'] + desired_order['orderQty'],
                                     'price': desired_order['price'], 'side': order['side']})
            except IndexError:
                # Will throw if there isn't a desired order to match. In that case, cancel it.
                to_cancel.append(order)

        while buys_matched < len(buy_orders):
            to_create.append(buy_orders[buys_matched])
            buys_matched += 1

        while sells_matched < len(sell_orders):
            to_create.append(sell_orders[sells_matched])
            sells_matched += 1

        if len(to_amend) > 0:
            for amended_order in reversed(to_amend):
                reference_order = [o for o in existing_orders if o['orderID'] == amended_order['orderID']][0]
                logger.info("Amending %4s: %d @ %.*f to %d @ %.*f (%+.*f)" % (
                    amended_order['side'],
                    reference_order['leavesQty'], tickLog, reference_order['price'],
                    (amended_order['orderQty'] - reference_order['cumQty']), tickLog, amended_order['price'],
                    tickLog, (amended_order['price'] - reference_order['price'])
                ))
            # This can fail if an order has closed in the time we were processing.
            # The API will send us `invalid ordStatus`, which means that the order's status (Filled/Canceled)
            # made it not amendable.
            # If that happens, we need to catch it and re-tick.
            try:
                self.exchange.amend_bulk_orders(to_amend)
            except requests.exceptions.HTTPError as e:
                errorObj = e.response.json()
                if errorObj['error']['message'] == 'Invalid ordStatus':
                    logger.warn("Amending failed. Waiting for order data to converge and retrying.")
                    sleep(0.5)
                    return self.place_orders()
                else:
                    logger.error("Unknown error on amend: %s. Exiting" % errorObj)
                    sys.exit()

        if len(to_create) > 0:
            logger.info("Creating %d orders:" % (len(to_create)))
            for order in reversed(to_create):
                logger.info("%4s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
            self.exchange.create_bulk_orders(to_create)

        # Could happen if we exceed a delta limit
        if len(to_cancel) > 0:
            logger.info("Canceling %d orders:" % (len(to_cancel)))
            for order in reversed(to_cancel):
                logger.info("%4s %d @ %.*f" % (order['side'], order['leavesQty'], tickLog, order['price']))
            self.exchange.cancel_bulk_orders(to_cancel)

    ###
    # Position Limits
    ###

    def short_position_limit_exceeded(self):
        """Returns True if the short position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.exchange.get_delta()
        return position <= settings.MIN_POSITION

    def long_position_limit_exceeded(self):
        """Returns True if the long position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.exchange.get_delta()
        return position >= settings.MAX_POSITION

    ###
    # Sanity
    ##

    def sanity_check(self):
        """Perform checks before placing orders."""

        # Check if OB is empty - if so, can't quote.
        self.exchange.check_if_orderbook_empty()

        # Ensure market is still open.
        self.exchange.check_market_open()

        # Get ticker, which sets price offsets and prints some debugging info.
        ticker = self.get_ticker()

        # Sanity check:
        if self.get_price_offset(-1) >= ticker["sell"] or self.get_price_offset(1) <= ticker["buy"]:
            logger.error("Buy: %s, Sell: %s" % (self.start_position_buy, self.start_position_sell))
            logger.error("First buy position: %s\nBitMEX Best Ask: %s\nFirst sell position: %s\nBitMEX Best Bid: %s" %
                         (self.get_price_offset(-1), ticker["sell"], self.get_price_offset(1), ticker["buy"]))
            logger.error("Sanity check failed, exchange data is inconsistent")
            self.exit()

        # Messaging if the position limits are reached
        if self.long_position_limit_exceeded():
            logger.info("Long delta limit exceeded")
            logger.info("Current Position: %.f, Maximum Position: %.f" %
                        (self.exchange.get_delta(), settings.MAX_POSITION))

        if self.short_position_limit_exceeded():
            logger.info("Short delta limit exceeded")
            logger.info("Current Position: %.f, Minimum Position: %.f" %
                        (self.exchange.get_delta(), settings.MIN_POSITION))

    ###
    # Running
    ###

    def check_file_change(self):
        """Restart if any files we're watching have changed."""
        for f, mtime in watched_files_mtimes:
            if getmtime(f) > mtime:
                self.restart()

    def check_connection(self):
        """Ensure the WS connections are still open."""
        return self.exchange.is_open()

    def exit(self):
        logger.info("Shutting down. All open orders will be cancelled.")
        try:
            self.exchange.cancel_all_orders()
            self.exchange.bitmex.exit()
        except errors.AuthenticationError as e:
            logger.info("Was not authenticated; could not cancel orders.")
        except Exception as e:
            logger.info("Unable to cancel orders: %s" % e)

        sys.exit()

    def run_loop(self):
        while True:
            sys.stdout.write("-----\n")
            sys.stdout.flush()

            self.check_file_change()
            sleep(settings.LOOP_INTERVAL)

            # This will restart on very short downtime, but if it's longer,
            # the MM will crash entirely as it is unable to connect to the WS on boot.
            if not self.check_connection():
                logger.error("Realtime data connection unexpectedly closed, restarting.")
                self.restart()

            self.sanity_check()  # Ensures health of mm - several cut-out points here
            self.print_status()  # Print skew, delta, etc
            self.place_orders()  # Creates desired orders and converges to existing orders

    def restart(self):
        logger.info("Restarting the market maker...")
        sys.exit()
        #os.execv(sys.executable, [sys.executable] + sys.argv)

#
# Helpers
#


def XBt_to_XBT(XBt):
    return float(XBt) / constants.XBt_TO_XBT


def cost(instrument, quantity, price):
    mult = instrument["multiplier"]
    P = mult * price if mult >= 0 else mult / price
    return abs(quantity * P)


def margin(instrument, quantity, price):
    return cost(instrument, quantity, price) * instrument["initMargin"]


def run():
    logger.info('BitMEX Market Maker Version: %s\n' % constants.VERSION)

    om = OrderManager()
    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        om.run_loop()
    except (KeyboardInterrupt, SystemExit) as e:
        logger.exception(e)
        sys.exit()
