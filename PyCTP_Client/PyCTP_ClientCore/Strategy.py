# -*- coding: utf-8 -*-
"""
Created on Wed Jul 20 08:46:13 2016

@author: YuWanying
"""


import copy
from PyCTP_Trade import PyCTP_Trader_API
from PyCTP_Market import PyCTP_Market_API
from OrderAlgorithm import OrderAlgorithm
import PyCTP
import time
import Utils


class Strategy:
    # class Strategy功能:接收行情，接收Json数据，触发交易信号，将交易任务交给OrderAlgorithm
    def __init__(self, dict_arguments, obj_user, obj_DBM):
        print("创建Strategy实例的形参", dict_arguments)
        print('创建Strategy: strategy_id=', dict_arguments['strategy_id'], 'BrokerID=', 'user_id=', dict_arguments['user_id'])
        self.__DBM = obj_DBM  # 数据库连接实例
        self.__user = obj_user  # user实例
        self.__dict_arguments = dict_arguments  # 转存形参到类的私有变量
        self.__TradingDay = self.__user.GetTradingDay()  # 获取交易日

        # 交易参数
        self.__trader_id = dict_arguments['trader_id']
        self.__user_id = dict_arguments['user_id']
        self.__strategy_id = dict_arguments['strategy_id']
        self.__order_algorithm = dict_arguments['order_algorithm']  # 下单算法选择标志位
        self.__list_instrument_id = dict_arguments['list_instrument_id']  # 合约列表
        self.__buy_open = dict_arguments['buy_open']  # 触发买开（开多单）
        self.__sell_close = dict_arguments['sell_close']  # 触发卖平（平多单）
        self.__sell_open = dict_arguments['sell_open']  # 触发卖开（开空单）
        self.__buy_close = dict_arguments['buy_close']  # 触发买平（平空单）
        self.__spread_shift = dict_arguments['spread_shift']  # 价差让价
        self.__a_wait_price_tick = dict_arguments['a_wait_price_tick']  # A合约挂单等待最小跳数
        self.__b_wait_price_tick = dict_arguments['b_wait_price_tick']  # B合约挂单等待最小跳数
        self.__stop_loss = dict_arguments['stop_loss']  # 止损，单位为最小跳数
        self.__lots = dict_arguments['lots']  # 总手
        self.__lots_batch = dict_arguments['lots_batch']  # 每批下单手数
        self.__is_active = dict_arguments['is_active']  # 策略开关状态
        self.__order_action_limit = dict_arguments['order_action_limit']  # 撤单次数限制
        self.__only_close = dict_arguments['only_close']  # 只能平仓
        self.__on_off = dict_arguments['on_off']  # 策略开关，0关，1开

        # 以一天的盘面为周期的统计指标
        self.__last_trade_date = dict_arguments['trade_date']  # 交易日
        self.__today_profit = dict_arguments['today_profit']  # 平仓盈利
        self.__today_commission = dict_arguments['today_commission']  # 手续费
        self.__today_trade_volume = dict_arguments['today_trade_volume']  # 成交量
        self.__today_sum_slippage = dict_arguments['today_sum_slippage']  # 总滑价
        self.__today_average_slippage = dict_arguments['today_average_slippage']  # 平均滑价

        self.__position_a_buy_today = dict_arguments['position_a_buy_today']  # A合约买持仓今仓
        self.__position_a_buy_yesterday = dict_arguments['position_a_buy_yesterday']  # A合约买持仓昨仓
        self.__position_a_buy = dict_arguments['position_a_buy']  # A合约买持仓总仓位
        self.__position_a_sell_today = dict_arguments['position_a_sell_today']  # A合约卖持仓今仓
        self.__position_a_sell_yesterday = dict_arguments['position_a_sell_yesterday']  # A合约卖持仓昨仓
        self.__position_a_sell = dict_arguments['position_a_sell']  # A合约卖持仓总仓位
        self.__position_b_buy_today = dict_arguments['position_b_buy_today']  # B合约买持仓今仓
        self.__position_b_buy_yesterday = dict_arguments['position_b_buy_yesterday']  # B合约买持仓昨仓
        self.__position_b_buy = dict_arguments['position_b_buy']  # B合约买持仓总仓位
        self.__position_b_sell_today = dict_arguments['position_b_sell_today']  # B合约卖持仓今仓
        self.__position_b_sell_yesterday = dict_arguments['position_b_sell_yesterday']  # B合约卖持仓昨仓
        self.__position_b_sell = dict_arguments['position_b_sell']  # B合约卖持仓总仓位    
        self.__list_position_detail = list()  # 持仓明细列表

        self.__instrument_a_tick = None  # A合约tick（第一腿）
        self.__instrument_b_tick = None  # B合约tick（第二腿）
        self.__spread_long = None  # 市场多头价差：A合约买一价 - B合约买一价
        self.__spread_long_volume = None  # 市场多头价差盘口挂单量min(A合约买一量 - B合约买一量)
        self.__spread_short = None  # 市场空头价差：A合约卖一价 - B合约卖一价
        self.__spread_short_volume = None  # 市场空头价差盘口挂单量：min(A合约买一量 - B合约买一量)
        self.__spread = None  # 市场最新价价差

        self.__order_ref_last = None  # 最后一次实际使用的报单引用
        self.__order_ref_a = None  # A合约报单引用
        self.__order_ref_b = None  # B合约报单引用

        self.__user.add_instrument_id_action_counter(dict_arguments['list_instrument_id'])  # 将合约代码添加到user类的合约列表

        self.__a_price_tick = self.get_price_tick(self.__list_instrument_id[0])  # A合约最小跳价
        self.__b_price_tick = self.get_price_tick(self.__list_instrument_id[1])  # B合约最小跳价

        # set_arguments()中不存在的私有变量
        self.__trade_tasking = False  # 交易任务进行中
        self.__a_order_insert_args = dict()  # a合约报单参数
        self.__b_order_insert_args = dict()  # b合约报单参数
        self.__list_order_pending = list()  # 挂单列表，报单、成交、撤单回报

    # 设置参数
    def set_arguments(self, dict_arguments):
        self.__dict_arguments = dict_arguments  # 将形参转存为私有变量
        self.__DBM.update_strategy(dict_arguments)  # 更新数据库
        self.__dict_arguments = dict_arguments  # 转存形参到类的私有变量

        self.__trader_id = dict_arguments['trader_id']
        self.__user_id = dict_arguments['user_id']
        self.__strategy_id = dict_arguments['strategy_id']
        self.__order_algorithm = dict_arguments['order_algorithm']  # 下单算法选择标志位
        self.__list_instrument_id = dict_arguments['list_instrument_id']  # 合约列表
        self.__buy_open = dict_arguments['buy_open']  # 触发买开（开多单）
        self.__sell_close = dict_arguments['sell_close']  # 触发卖平（平多单）
        self.__sell_open = dict_arguments['sell_open']  # 触发卖开（开空单）
        self.__buy_close = dict_arguments['buy_close']  # 触发买平（平空单）
        self.__spread_shift = dict_arguments['spread_shift']  # 价差让价
        self.__a_wait_price_tick = dict_arguments['a_wait_price_tick']  # A合约挂单等待最小跳数
        self.__b_wait_price_tick = dict_arguments['b_wait_price_tick']  # B合约挂单等待最小跳数
        self.__stop_loss = dict_arguments['stop_loss']  # 止损，单位为最小跳数
        self.__lots = dict_arguments['lots']  # 总手
        self.__lots_batch = dict_arguments['lots_batch']  # 每批下单手数
        self.__is_active = dict_arguments['is_active']  # 策略开关状态
        self.__order_action_limit = dict_arguments['order_action_limit']  # 撤单次数限制
        self.__only_close = dict_arguments['only_close']  # 只能平仓
        self.__on_off = dict_arguments['on_off']  # 策略开关，0关，1开

        # 以一天的盘面为周期的统计指标
        self.__last_trade_date = dict_arguments['trade_date']  # 交易日
        self.__today_profit = dict_arguments['today_profit']  # 平仓盈利
        self.__today_commission = dict_arguments['today_commission']  # 手续费
        self.__today_trade_volume = dict_arguments['commission']  # 成交量
        self.__today_sum_slippage = dict_arguments['today_sum_slippage']  # 总滑价
        self.__today_average_slippage = dict_arguments['today_average_slippage']  # 平均滑价

        self.__position_a_buy_today = dict_arguments['position_a_buy_today']  # A合约买持仓今仓
        self.__position_a_buy_yesterday = dict_arguments['position_a_buy_yesterday']  # A合约买持仓昨仓
        self.__position_a_buy = dict_arguments['position_a_buy']  # A合约买持仓总仓位
        self.__position_a_sell_today = dict_arguments['position_a_sell_today']  # A合约卖持仓今仓
        self.__position_a_sell_yesterday = dict_arguments['position_a_sell_yesterday']  # A合约卖持仓昨仓
        self.__position_a_sell = dict_arguments['position_a_sell']  # A合约卖持仓总仓位
        self.__position_b_buy_today = dict_arguments['position_b_buy_today']  # B合约买持仓今仓
        self.__position_b_buy_yesterday = dict_arguments['position_b_buy_yesterday']  # B合约买持仓昨仓
        self.__position_b_buy = dict_arguments['position_b_buy']  # B合约买持仓总仓位
        self.__position_b_sell_today = dict_arguments['position_b_sell_today']  # B合约卖持仓今仓
        self.__position_b_sell_yesterday = dict_arguments['position_b_sell_yesterday']  # B合约卖持仓昨仓
        self.__position_b_sell = dict_arguments['position_b_sell']  # B合约卖持仓总仓位    
        self.__list_position_detail = list()  # 持仓明细列表

        self.__user.add_instrument_id_action_counter(dict_arguments['list_instrument_id'])  # 将合约代码添加到user类的合约列表

        self.__a_price_tick = self.get_price_tick(self.__list_instrument_id[0])  # A合约最小跳价
        self.__b_price_tick = self.get_price_tick(self.__list_instrument_id[1])  # B合约最小跳价

        self.__order_ref_last = None  # 最后一次实际使用的报单引用
        self.__order_ref_a = None  # A合约报单引用
        self.__order_ref_b = None  # B合约报单引用

    # 获取参数
    def get_arguments(self):
        return self.__dict_arguments

    # 设置数据库连接实例
    def set_DBM(self, DBM):
        self.__DBM = DBM

    # 获取数据库连接实例
    def get_DBM(self):
        return self.__DBM

    # 设置user对象
    def set_user(self, user):
        self.__user = user

    # 获取user对象
    def get_user(self, user):
        return self.__user

    # 获取trader_id
    def get_trader_id(self):
        return self.__trader_id

    # 获取user_id
    def get_user_id(self):
        return self.__user_id

    # 获取strategy_id
    def get_strategy_id(self):
        return self.__strategy_id

    # 获取self.__list_instrument_id
    def get_list_instrument_id(self):
        return self.__list_instrument_id

    # 获取指定合约最小跳'PriceTick'
    def get_price_tick(self, instrument_id):
        print("Strategy.get_price_tick() type(self.__user.get_instrument_info())=", type(self.__user.get_instrument_info()), self.__user.get_instrument_info())
        for i in self.__user.get_instrument_info():
            if i['InstrumentID'] == instrument_id:
                return i['PriceTick']

    # 生成报单引用，前两位是策略编号，后面几位递增1
    def add_order_ref(self):
        return (str(self.__user.add_order_ref_part2()) + self.__strategy_id).encode()

    # 回调函数：行情推送
    def OnRtnDepthMarketData(self, tick):
        """ 行情推送 """
        # 过滤出B合约的tick
        if tick['InstrumentID'] == self.__list_instrument_id[1]:
            self.__instrument_b_tick = tick
            # print(self.__user_id + self.__strategy_id, "B合约：", self.__instrument_b_tick)
        # 过滤出A合约的tick
        elif tick['InstrumentID'] == self.__list_instrument_id[0]:
            self.__instrument_a_tick = tick
            # print(self.__user_id + self.__strategy_id, "A合约：", self.__instrument_a_tick)

        # 没有下单任务执行中，进入下单算法
        if not self.__trade_tasking:
            self.select_order_algorithm(self.__order_algorithm)
        # 有下单任务执行中，行情跟踪
        elif self.__trade_tasking:
            dict_arguments = {'flag': 'tick', 'tick': tick}
            self.trade_task(dict_arguments)
        # 打印价差
        # print(self.__user_id + self.__strategy_id, self.__list_instrument_id, self.__spread_long, "(", self.__spread_long_volume, ")", self.__spread_short, "(", self.__spread_short_volume, ")")

    def OnRspOrderInsert(self, InputOrder, RspInfo, RequestID, IsLast):
        """ 报单录入请求响应 """
        # 报单错误时响应
        if Utils.Strategy_print:
            print('Strategy.OnRspOrderInsert()', 'OrderRef:', InputOrder['OrderRef'], 'InputOrder:', InputOrder, 'RspInfo:', RspInfo, 'RequestID:', RequestID, 'IsLast:', IsLast)
        dict_arguments = {'flag': 'OnRspOrderInsert',
                          'InputOrder': InputOrder,
                          'RspInfo': RspInfo,
                          'RequestID': RequestID,
                          'IsLast': IsLast}
        self.trade_task(dict_arguments)  # 转到交易任务处理

    def OnRspOrderAction(self, InputOrderAction, RspInfo, RequestID, IsLast):
        """报单操作请求响应:撤单操作响应"""
        if Utils.Strategy_print:
            print('Strategy.OnRspOrderAction()', 'OrderRef:', InputOrderAction['OrderRef'], 'InputOrderAction:', InputOrderAction, 'RspInfo:', RspInfo, 'RequestID:', RequestID, 'IsLast:', IsLast)
        dict_arguments = {'flag': 'OnRspOrderAction',
                          'InputOrderAction': InputOrderAction,
                          'RspInfo': RspInfo,
                          'RequestID': RequestID,
                          'IsLast': IsLast}
        self.trade_task(dict_arguments)  # 转到交易任务处理

    def OnRtnOrder(self, Order):
        """报单回报"""
        from User import User
        if Utils.Strategy_print:
            print('Strategy.OnRtnOrder()', 'OrderRef:', Order['OrderRef'], 'Order', Order)

        dict_arguments = {'flag': 'OnRtnOrder',
                          'Order': Order}

        self.update_list_order_pending(dict_arguments)  # 更新挂单list
        self.update_task_status()  # 更新任务状态
        # self.update_position(dict_arguments)  # 更新持仓量变量（放到OnRtnTrade回调中）

        self.__user.action_counter(dict_arguments['Order']['InstrumentID'])  # 撤单次数添加到user类的撤单计数器
        self.trade_task(dict_arguments)  # 转到交易任务处理

    def OnRtnTrade(self, Trade):
        """成交回报"""
        if Utils.Strategy_print:
            print('Strategy.OnRtnTrade()', 'OrderRef:', Trade['OrderRef'], 'Trade', Trade)

        self.update_list_position_detail(Trade)  # 更新持仓明细list

        dict_arguments = {'flag': 'OnRtnTrade',
                          'Trade': Trade}

        self.update_position(Trade)  # 更新持仓量变量
        self.update_task_status()  # 更新任务状态
        
        self.trade_task(dict_arguments)  # 转到交易任务处理

    def OnErrRtnOrderAction(self, OrderAction, RspInfo):
        """ 报单操作错误回报 """
        if Utils.Strategy_print:
            print('Strategy.OnErrRtnOrderAction()', 'OrderRef:', OrderAction['OrderRef'], 'OrderAction:', OrderAction, 'RspInfo:', RspInfo)
        dict_arguments = {'flag': 'OnErrRtnOrderAction',
                          'OrderAction': OrderAction,
                          'RspInfo': RspInfo}
        self.trade_task(dict_arguments)  # 转到交易任务处理

    def OnErrRtnOrderInsert(self, InputOrder, RspInfo):
        """报单录入错误回报"""
        if Utils.Strategy_print:
            print('Strategy.OnErrRtnOrderInsert()', 'OrderRef:', InputOrder['OrderRef'], 'InputOrder:', InputOrder, 'RspInfo:', RspInfo)
        dict_arguments = {'flag': 'OnErrRtnOrderInsert',
                          'InputOrder': InputOrder,
                          'RspInfo': RspInfo}
        self.trade_task(dict_arguments)  # 转到交易任务处理

    # 选择下单算法
    def select_order_algorithm(self, flag):
        # 有挂单
        if len(self.__list_order_pending) > 0:
            return
        # 撇退
        if self.__position_a_sell != self.__position_b_buy or self.__position_a_buy != self.__position_b_sell:
            return
        # 选择执行交易算法
        if flag == '01':
            self.order_algorithm_one()
        elif flag == '02':
            self.order_algorithm_two()
        elif flag == '03':
            self.order_algorithm_three()
        else:
            print("Strategy.select_order_algorithm() 没有选择下单算法")

    # 下单算法1：A合约以对手价发单，B合约以对手价发单
    def order_algorithm_one(self):
        # 计算市场盘口价差、量
        if self.__instrument_a_tick is not None and self.__instrument_b_tick is not None:
            self.__spread_long = self.__instrument_a_tick['BidPrice1'] - self.__instrument_b_tick['AskPrice1']
            self.__spread_long_volume = min(self.__instrument_a_tick['BidVolume1'],
                                            self.__instrument_b_tick['AskVolume1'])
            self.__spread_short = self.__instrument_a_tick['AskPrice1'] - self.__instrument_b_tick['BidPrice1']
            self.__spread_short_volume = min(self.__instrument_a_tick['AskVolume1'],
                                             self.__instrument_b_tick['BidVolume1'])
            if Utils.Strategy_print:
                print("Strategy.order_algorithm_one() 策略编号", self.__user_id + self.__strategy_id, self.__list_instrument_id, self.__spread_long, "(", self.__spread_long_volume, ")", self.__spread_short, "(", self.__spread_short_volume, ")")
        else:
            return

        # 策略开关为关则直接跳出，不执行开平仓逻辑判断，依次为：策略开关、单个期货账户开关（user）、总开关（trader）
        if self.__on_off == 0 or self.__user.get_on_off() == 0 or self.__user.get_CTPManager().get_on_off() == 0:
            print("Strategy.order_algorithm_one() 策略开关状态", self.__on_off, self.__user.get_on_off(), self.__user.get_CTPManager().get_on_off())
            return

        # 价差卖平
        if self.__spread_long <= self.__sell_close \
                and self.__position_a_sell == self.__position_b_buy \
                and self.__position_a_sell > 0:
            '''
                市场多头价差大于等于策略卖平触发参数
                A、B合约持仓量相等且大于0
            '''
            if Utils.Strategy_print:
                print("Strategy.order_algorithm_one() 策略编号", self.__strategy_id, "交易信号触发", "价差买平")
            # 打印价差
            if Utils.Strategy_print:
                print("Strategy.order_algorithm_one() 策略编号", self.__user_id + self.__strategy_id,
                      self.__list_instrument_id, self.__spread_long, "(", self.__spread_long_volume, ")",
                      self.__spread_short, "(", self.__spread_short_volume, ")")
            # 满足交易任务之前的一个tick
            self.__instrument_a_tick_after_tasking = self.__instrument_a_tick
            self.__instrument_b_tick_after_tasking = self.__instrument_b_tick
            # 优先平昨仓
            # 报单手数：盘口挂单量、每份发单手数、持仓量
            if self.__position_a_sell_yesterday > 0:
                order_volume = min(self.__spread_short_volume,
                                   self.__lots_batch,
                                   self.__position_a_sell_yesterday)
                CombOffsetFlag = b'4'  # 平昨标志
            elif self.__position_a_sell_yesterday == 0 and self.__position_a_sell_today > 0:
                order_volume = min(self.__spread_short_volume,
                                   self.__lots_batch,
                                   self.__position_a_sell_today)
                CombOffsetFlag = b'3'  # 平今标志
            if order_volume <= 0 or not isinstance(order_volume, int):
                if Utils.Strategy_print:
                    print('Strategy.order_algorithm_one() 发单手数错误值', order_volume)
            self.__order_ref_a = self.add_order_ref()  # 报单引用
            self.__order_ref_last = self.__order_ref_a
            # A合约报单参数，全部确定
            self.__a_order_insert_args = {'flag': 'OrderInsert',  # 标志位：报单
                                          'OrderRef': self.__order_ref_a,  # 报单引用
                                          'InstrumentID': self.__list_instrument_id[0].encode(),  # 合约代码
                                          'LimitPrice': self.__instrument_a_tick['AskPrice1'],  # 限价
                                          'VolumeTotalOriginal': order_volume,  # 数量
                                          'Direction': b'0',  # 买卖，0买,1卖
                                          'CombOffsetFlag': CombOffsetFlag,  # 组合开平标志，0开仓，上期所3平今、4平昨，其他交易所1平仓
                                          'CombHedgeFlag': b'1',  # 组合投机套保标志:1投机、2套利、3保值
                                          }
            # B合约报单参数，部分确定，报单引用和报单数量，根据A合约成交情况确定
            self.__b_order_insert_args = {'flag': 'OrderInsert',  # 标志位：报单
                                          # 'OrderRef': self.__order_ref_b,  # 报单引用
                                          'InstrumentID': self.__list_instrument_id[1].encode(),  # 合约代码
                                          'LimitPrice': self.__instrument_b_tick['BidPrice1'],  # 限价
                                          # 'VolumeTotalOriginal': order_volume,  # 数量
                                          'Direction': b'1',  # 买卖，0买,1卖
                                          'CombOffsetFlag': CombOffsetFlag,  # 组合开平标志，0开仓，上期所3平今、4平昨，其他交易所1平仓
                                          'CombHedgeFlag': b'1',  # 组合投机套保标志:1投机、2套利、3保值
                                          }
            self.trade_task(self.__a_order_insert_args)  # 执行下单任务
            self.__trade_tasking = True  # 交易任务执行中
        # 价差买平
        elif self.__spread_short <= self.__buy_close\
                and self.__position_a_sell == self.__position_b_buy\
                and self.__position_a_sell > 0:
            '''
                市场空头价差小于等于策略买平触发参数
                A、B合约持仓量相等且大于0
            '''
            if Utils.Strategy_print:
                print("Strategy.order_algorithm_one() 策略编号", self.__strategy_id, "交易信号触发", "价差买平")
            # 打印价差
            if Utils.Strategy_print:
                print("Strategy.order_algorithm_one() 策略编号", self.__user_id + self.__strategy_id, self.__list_instrument_id, self.__spread_long, "(", self.__spread_long_volume, ")", self.__spread_short, "(", self.__spread_short_volume, ")")
            # 满足交易任务之前的一个tick
            self.__instrument_a_tick_after_tasking = self.__instrument_a_tick
            self.__instrument_b_tick_after_tasking = self.__instrument_b_tick
            # 优先平昨仓
            # 报单手数：盘口挂单量、每份发单手数、持仓量
            if self.__position_a_sell_yesterday > 0:
                order_volume = min(self.__spread_short_volume,
                                   self.__lots_batch,
                                   self.__position_a_sell_yesterday)
                CombOffsetFlag = b'4'  # 平昨标志
            elif self.__position_a_sell_yesterday == 0 and self.__position_a_sell_today > 0:
                order_volume = min(self.__spread_short_volume,
                                   self.__lots_batch,
                                   self.__position_a_sell_today)
                CombOffsetFlag = b'3'  # 平今标志
            if order_volume <= 0 or not isinstance(order_volume, int):
                if Utils.Strategy_print:
                    print('Strategy.order_algorithm_one() 发单手数错误值', order_volume)
            self.__order_ref_a = self.add_order_ref()  # 报单引用
            self.__order_ref_last = self.__order_ref_a
            # A合约报单参数，全部确定
            self.__a_order_insert_args = {'flag': 'OrderInsert',  # 标志位：报单
                                          'OrderRef': self.__order_ref_a,  # 报单引用
                                          'InstrumentID': self.__list_instrument_id[0].encode(),  # 合约代码
                                          'LimitPrice': self.__instrument_a_tick['AskPrice1'],  # 限价
                                          'VolumeTotalOriginal': order_volume,  # 数量
                                          'Direction': b'0',  # 买卖，0买,1卖
                                          'CombOffsetFlag': CombOffsetFlag,  # 组合开平标志，0开仓，上期所3平今、4平昨，其他交易所1平仓
                                          'CombHedgeFlag': b'1',  # 组合投机套保标志:1投机、2套利、3保值
                                          }
            # B合约报单参数，部分确定，报单引用和报单数量，根据A合约成交情况确定
            self.__b_order_insert_args = {'flag': 'OrderInsert',  # 标志位：报单
                                          # 'OrderRef': self.__order_ref_b,  # 报单引用
                                          'InstrumentID': self.__list_instrument_id[1].encode(),  # 合约代码
                                          'LimitPrice': self.__instrument_b_tick['BidPrice1'],  # 限价
                                          # 'VolumeTotalOriginal': order_volume,  # 数量
                                          'Direction': b'1',  # 买卖，0买,1卖
                                          'CombOffsetFlag': CombOffsetFlag,  # 组合开平标志，0开仓，上期所3平今、4平昨，其他交易所1平仓
                                          'CombHedgeFlag': b'1',  # 组合投机套保标志:1投机、2套利、3保值
                                          }
            self.trade_task(self.__a_order_insert_args)  # 执行下单任务
            self.__trade_tasking = True  # 交易任务执行中
        # 价差卖开
        elif self.__spread_long >= self.__sell_open\
                and self.__position_a_buy + self.__position_a_sell < self.__lots:
            '''
            市场多头价差大于策略卖开触发参数
            策略多头持仓量+策略空头持仓量小于策略参数总手
            '''
            if Utils.Strategy_print:
                print("Strategy.order_algorithm_one() 策略编号", self.__strategy_id, "交易信号触发", "价差卖开")
            # 打印价差
            if Utils.Strategy_print:
                print(self.__user_id + self.__strategy_id, self.__list_instrument_id, self.__spread_long, "(", self.__spread_long_volume, ")", self.__spread_short, "(", self.__spread_short_volume, ")")
            # 满足交易任务之前的一个tick
            self.__instrument_a_tick_after_tasking = self.__instrument_a_tick
            self.__instrument_b_tick_after_tasking = self.__instrument_b_tick
            # 报单手数：盘口挂单量、每份发单手数、剩余可开仓手数中取最小值
            order_volume = min(self.__spread_long_volume,  # 市场对手量
                               self.__lots_batch,  # 每份量
                               self.__lots - (self.__position_a_buy + self.__position_b_buy))  # 剩余可开数量
            if order_volume <= 0 or not isinstance(order_volume, int):
                if Utils.Strategy_print:
                    print('Strategy.order_algorithm_one() 发单手数错误值', order_volume)
            self.__order_ref_a = self.add_order_ref()  # 报单引用
            self.__order_ref_last = self.__order_ref_a
            # A合约报单参数，全部确定
            self.__a_order_insert_args = {'flag': 'OrderInsert',  # 标志位：报单
                                          'OrderRef': self.__order_ref_a,  # 报单引用
                                          'InstrumentID': self.__list_instrument_id[0].encode(),  # 合约代码
                                          'LimitPrice': self.__instrument_a_tick['BidPrice1'],  # 限价
                                          'VolumeTotalOriginal': order_volume,  # 数量
                                          'Direction': b'1',  # 买卖，0买,1卖
                                          'CombOffsetFlag': b'0',  # 组合开平标志，0开仓，上期所3平今、4平昨，其他交易所1平仓
                                          'CombHedgeFlag': b'1',  # 组合投机套保标志:1投机、2套利、3保值
                                          }
            # B合约报单参数，部分确定，报单引用和报单数量，根据A合约成交情况确定
            self.__b_order_insert_args = {'flag': 'OrderInsert',  # 标志位：报单
                                          # 'OrderRef': self.__order_ref_b,  # 报单引用
                                          'InstrumentID': self.__list_instrument_id[1].encode(),  # 合约代码
                                          'LimitPrice': self.__instrument_b_tick['AskPrice1'],  # 限价
                                          # 'VolumeTotalOriginal': order_volume,  # 数量
                                          'Direction': b'0',  # 买卖，0买,1卖
                                          'CombOffsetFlag': b'0',  # 组合开平标志，0开仓，上期所3平今、4平昨，其他交易所1平仓
                                          'CombHedgeFlag': b'1',  # 组合投机套保标志:1投机、2套利、3保值
                                          }
            self.trade_task(self.__a_order_insert_args)  # 执行下单任务
            self.__trade_tasking = True  # 交易任务执行中
        # 价差买开
        elif self.__spread_short <= self.__buy_open \
                and self.__position_a_buy + self.__position_a_sell < self.__lots:
            '''
            市场空头价差小于策略买开触发参数
            策略多头持仓量+策略空头持仓量小于策略参数总手
            '''
            if Utils.Strategy_print:
                print("Strategy.order_algorithm_one() 策略编号", self.__strategy_id, "交易信号触发", "价差卖开")
            # 打印价差
            if Utils.Strategy_print:
                print(self.__user_id + self.__strategy_id, self.__list_instrument_id, self.__spread_long, "(",
                      self.__spread_long_volume, ")", self.__spread_short, "(", self.__spread_short_volume, ")")
            # 满足交易任务之前的一个tick
            self.__instrument_a_tick_after_tasking = self.__instrument_a_tick
            self.__instrument_b_tick_after_tasking = self.__instrument_b_tick
            # 报单手数：盘口挂单量、每份发单手数、剩余可开仓手数中取最小值
            order_volume = min(self.__spread_long_volume,  # 市场对手量
                               self.__lots_batch,  # 每份量
                               self.__lots - (self.__position_a_buy + self.__position_b_buy))  # 剩余可开数量
            if order_volume <= 0 or not isinstance(order_volume, int):
                if Utils.Strategy_print:
                    print('Strategy.order_algorithm_one() 发单手数错误值', order_volume)
            self.__order_ref_a = self.add_order_ref()  # 报单引用
            self.__order_ref_last = self.__order_ref_a
            # A合约报单参数，全部确定
            self.__a_order_insert_args = {'flag': 'OrderInsert',  # 标志位：报单
                                          'OrderRef': self.__order_ref_a,  # 报单引用
                                          'InstrumentID': self.__list_instrument_id[0].encode(),  # 合约代码
                                          'LimitPrice': self.__instrument_a_tick['BidPrice1'],  # 限价
                                          'VolumeTotalOriginal': order_volume,  # 数量
                                          'Direction': b'0',  # 买卖，0买,1卖
                                          'CombOffsetFlag': b'0',  # 组合开平标志，0开仓，上期所3平今、4平昨，其他交易所1平仓
                                          'CombHedgeFlag': b'1',  # 组合投机套保标志:1投机、2套利、3保值
                                          }
            # B合约报单参数，部分确定，报单引用和报单数量，根据A合约成交情况确定
            self.__b_order_insert_args = {'flag': 'OrderInsert',  # 标志位：报单
                                          # 'OrderRef': self.__order_ref_b,  # 报单引用
                                          'InstrumentID': self.__list_instrument_id[1].encode(),  # 合约代码
                                          'LimitPrice': self.__instrument_b_tick['AskPrice1'],  # 限价
                                          # 'VolumeTotalOriginal': order_volume,  # 数量
                                          'Direction': b'1',  # 买卖，0买,1卖
                                          'CombOffsetFlag': b'0',  # 组合开平标志，0开仓，上期所3平今、4平昨，其他交易所1平仓
                                          'CombHedgeFlag': b'1',  # 组合投机套保标志:1投机、2套利、3保值
                                          }
            self.trade_task(self.__a_order_insert_args)  # 执行下单任务
            self.__trade_tasking = True  # 交易任务执行中

    # 下单算法2：A合约以最新成交价发单，B合约以对手价发单
    def order_algorithm_two(self):
        if Utils.Strategy_print:
            print("Strategy.order_algorithm_two()")
        pass

    # 下单算法3：A合约以挂单价发单，B合约以对手价发单
    def order_algorithm_three(self):
        if Utils.Strategy_print:
            print("Strategy.order_algorithm_three()")
        pass

    def trade_task(self, dict_arguments):
        """"交易任务执行"""
        # 报单
        if dict_arguments['flag'] == 'OrderInsert':
            """交易任务开始入口"""
            if Utils.Strategy_print:
                print('Strategy.trade_task() A合约报单，OrderRef=', dict_arguments['OrderRef'], '报单参数：', dict_arguments)
            self.__user.get_trade().OrderInsert(dict_arguments)  # A合约报单
        # 报单录入请求响应
        elif dict_arguments['flag'] == 'OnRspOrderInsert':
            if Utils.Strategy_print:
                print("Strategy.trade_task() 报单录入请求响应")
            pass
        # 报单操作请求响应
        elif dict_arguments['flag'] == 'OnRspOrderAction':
            if Utils.Strategy_print:
                print("Strategy.trade_task() 报单操作请求响应")
            pass
        # 报单回报
        elif dict_arguments['flag'] == 'OnRtnOrder':
            # A成交回报，B发送等量的报单(OrderInsert)
            if dict_arguments['Order']['InstrumentID'] == self.__list_instrument_id[0] \
                    and dict_arguments['Order']['OrderStatus'] in ['0', '1']:  # OrderStatus全部成交或部分成交
                # 无挂单，当前报单回报中的VolumeTrade就是本次成交量
                if len(self.__list_order_pending) == 0:
                    self.__b_order_insert_args['VolumeTotalOriginal'] = dict_arguments['Order']['VolumeTraded']
                # 有挂单，从挂单列表中查找是否有相同的OrderRef
                else:
                    b_fined = False  # 是否找到的初始值
                    for i in self.__list_order_pending:
                        # 从挂单列表中找到相同的OrderRef记录，当前回报的VolumeTraded减去上一条回报中VolumeTrade等于本次成交量
                        if i['OrderRef'] == dict_arguments['Order']['OrderRef']:
                            self.__b_order_insert_args['VolumeTotalOriginal'] = \
                                dict_arguments['Order']['VolumeTraded'] - i['VolumeTraded']  # B发单量等于本次回报A的成交量
                            b_fined = True  # 找到了，赋值为真
                            break
                    # 未在挂单列表中找到相同的OrderRef记录，当前报单回报中的VolumeTraded就是本次成交量
                    if not b_fined:
                        self.__b_order_insert_args['VolumeTotalOriginal'] = dict_arguments['Order']['VolumeTraded']

                self.__order_ref_b = self.add_order_ref()  # B报单引用
                self.__order_ref_last = self.__order_ref_b  # 实际最后使用的报单引用
                self.__b_order_insert_args['OrderRef'] = self.__order_ref_b
                if Utils.Strategy_print:
                    print('Strategy.trade_task() B合约报单，OrderRef=', self.__b_order_insert_args['OrderRef'], '报单参数：', self.__b_order_insert_args)
                self.__user.get_trade().OrderInsert(self.__b_order_insert_args)  # B合约报单
            # B成交回报
            elif dict_arguments['Order']['InstrumentID'] == self.__list_instrument_id[1] \
                    and dict_arguments['Order']['OrderStatus'] in ['0', '1']:  # OrderStatus全部成交或部分成交
                pass
            # B撤单回报，启动B重新发单一定成交策略
            elif dict_arguments['Order']['InstrumentID'] == self.__list_instrument_id[1] \
                    and dict_arguments['Order']['OrderStatus'] == '5' \
                    and len(dict_arguments['Order']['OrderSysID']) == 12:
                if Utils.Strategy_print:
                    print("Strategy.trade_task() 策略编号：", self.__user_id+self.__strategy_id, "收到B撤单回报，启动B重新发单一定成交策略")
                self.__order_ref_b = self.add_order_ref()  # B报单引用
                self.__order_ref_last = self.__order_ref_b  # 实际最后使用的报单引用
                if dict_arguments['Order']['Direction'] == '0':
                    LimitPrice = self.__instrument_b_tick['AskPrice1']  # B报单价格，找市场最新对手价
                elif dict_arguments['Order']['Direction'] == '1':
                    LimitPrice = self.__instrument_b_tick['BidPrice1']
                self.__b_order_insert_args = {'flag': 'OrderInsert',  # 标志位：报单
                                              'OrderRef': self.__order_ref_b,  # 报单引用
                                              'InstrumentID': self.__list_instrument_id[1].encode(),  # 合约代码
                                              'LimitPrice': LimitPrice,  # 限价
                                              'VolumeTotalOriginal': dict_arguments['Order']['VolumeTotal'],  # 撤单回报中的剩余未成交数量
                                              'Direction': dict_arguments['Order']['Direction'].encode(),  # 买卖，0买,1卖
                                              'CombOffsetFlag': dict_arguments['Order']['CombOffsetFlag'].encode(),  # 组合开平标志，0开仓，上期所3平今、4平昨，其他交易所1平仓
                                              'CombHedgeFlag': dict_arguments['Order']['CombHedgeFlag'].encode(),  # 组合投机套保标志:1投机、2套利、3保值
                                              }

                if Utils.Strategy_print:
                    print('Strategy.trade_task() B合约报单，OrderRef=', self.__b_order_insert_args['OrderRef'], '报单参数：', self.__b_order_insert_args)
                self.__user.get_trade().OrderInsert(self.__b_order_insert_args)  # B合约报单
        # 报单录入错误回报
        elif dict_arguments['flag'] == 'OnErrRtnOrderInsert':
            if Utils.Strategy_print:
                print("Strategy.trade_task() 报单录入错误回报")
            pass
        # 报单操作错误回报
        elif dict_arguments['flag'] == 'OnErrRtnOrderAction':
            if Utils.Strategy_print:
                print("Strategy.trade_task() 报单操作错误回报")
            pass
        # 行情回调，并且交易任务进行中
        elif dict_arguments['flag'] == 'tick' and self.__trade_tasking:
            """当交易任务进行中时，判断是否需要撤单"""
            # print("Strategy.trade_task() tick驱动判断是否需要撤单")
            # 遍历挂单列表
            for i in self.__list_order_pending:
                # A有挂单，判断是否需要撤单
                if i['InstrumentID'] == self.__list_instrument_id[0]:
                    # 通过A最新tick判断A合约是否需要撤单
                    if dict_arguments['tick']['InstrumentID'] == self.__list_instrument_id[0]:
                        # A挂单的买卖方向为买
                        if i['Direction'] == '0':
                            # 挂单价格与盘口买一价比较，如果与盘口价格差距n个最小跳以上，撤单
                            # print("Strategy.trade_task()self.__a_wait_price_tick * self.__a_price_tick", self.__a_wait_price_tick, self.__a_price_tick,type(self.__a_wait_price_tick), type(self.__a_price_tick))
                            if dict_arguments['tick']['BidPrice1'] > (i['LimitPrice'] + self.__a_wait_price_tick*self.__a_price_tick):
                                if Utils.Strategy_print:
                                    print("Strategy.trade_task() 通过A最新tick判断A合约买挂单符合撤单条件")
                                # A合约撤单
                                order_action_arguments = {'OrderRef': i['OrderRef'].encode(),
                                                          'ExchangeID': i['ExchangeID'].encode(),
                                                          'OrderSysID': i['OrderSysID'].encode()}
                                if Utils.Strategy_print:
                                    print('Strategy.trade_task() A合约撤单，OrderRef=', i['OrderRef'], '撤单参数：', order_action_arguments)
                                self.__user.get_trade().OrderAction(order_action_arguments)
                        # A挂单的买卖方向为卖
                        elif i['Direction'] == '1':
                            # 挂单价格与盘口卖一价比较，如果与盘口价格差距n个最小跳以上，撤单
                            # print("Strategy.trade_task()self.__a_wait_price_tick * self.__a_price_tick", self.__a_wait_price_tick, self.__a_price_tick,type(self.__a_wait_price_tick), type(self.__a_price_tick))
                            if dict_arguments['tick']['AskPrice1'] <= (i['LimitPrice'] - self.__a_wait_price_tick * self.__a_price_tick):
                                if Utils.Strategy_print:
                                    print("Strategy.trade_task() 通过A最新tick判断A合约卖挂单符合撤单条件")
                                # A合约撤单
                                order_action_arguments = {'OrderRef': i['OrderRef'].encode(),
                                                          'ExchangeID': i['ExchangeID'].encode(),
                                                          'OrderSysID': i['OrderSysID'].encode()}
                                if Utils.Strategy_print:
                                    print('Strategy.trade_task()A合约撤单，OrderRef=', i['OrderRef'], '撤单参数：', order_action_arguments)
                                self.__user.get_trade().OrderAction(order_action_arguments)
                    # 通过B最新tick判断A合约是否需要撤单
                    elif dict_arguments['tick']['InstrumentID'] == self.__list_instrument_id[1]:
                        # A挂单的买卖方向为买
                        if i['Direction'] == '0':
                            # B最新tick的对手价如果与开仓信号触发时B的tick对手价发生不利变化则A撤单
                            if dict_arguments['tick']['BidPrice1'] < self.__instrument_b_tick_after_tasking['BidPrice1']:
                                if Utils.Strategy_print:
                                    print("Strategy.trade_task() 通过B最新tick判断A合约买挂单符合撤单条件")
                                # A合约撤单
                                order_action_arguments = {'OrderRef': i['OrderRef'].encode(),
                                                          'ExchangeID': i['ExchangeID'].encode(),
                                                          'OrderSysID': i['OrderSysID'].encode()}
                                if Utils.Strategy_print:
                                    print('Strategy.trade_task() A合约撤单，OrderRef=', i['OrderRef'], '撤单参数：', order_action_arguments)
                                self.__user.get_trade().OrderAction(order_action_arguments)
                        # A挂单的买卖方向为卖
                        elif i['Direction'] == '1':
                            # B最新tick的对手价如果与开仓信号触发时B的tick对手价发生不利变化则A撤单
                            if dict_arguments['tick']['AskPrice1'] > self.__instrument_b_tick_after_tasking['AskPrice1']:
                                if Utils.Strategy_print:
                                    print("Strategy.trade_task()通过B最新tick判断A合约卖挂单符合撤单条件")
                                # A合约撤单
                                order_action_arguments = {'OrderRef': i['OrderRef'].encode(),
                                                          'ExchangeID': i['ExchangeID'].encode(),
                                                          'OrderSysID': i['OrderSysID'].encode()}
                                if Utils.Strategy_print:
                                    print('Strategy.trade_task() A合约撤单，OrderRef=', i['OrderRef'], '撤单参数：', order_action_arguments)
                                self.__user.get_trade().OrderAction(order_action_arguments)
                # B有挂单，判断是否需要撤单，并启动B合约一定成交策略
                if i['InstrumentID'] == self.__list_instrument_id[1]:
                    # 通过B最新tick判断B合约是否需要撤单
                    if dict_arguments['tick']['InstrumentID'] == self.__list_instrument_id[1]:
                        # B挂单的买卖方向为买
                        if i['Direction'] == '0':
                            # 挂单价格与盘口买一价比较，如果与盘口价格差距n个最小跳以上，撤单
                            if Utils.Strategy_print:
                                print("Strategy.trade_task() self.__b_wait_price_tick * self.__b_price_tick", self.__b_wait_price_tick, self.__b_price_tick, type(self.__b_wait_price_tick), type(self.__b_price_tick))
                            if dict_arguments['tick']['BidPrice1'] >= (i['LimitPrice'] + self.__b_wait_price_tick * self.__b_price_tick):
                                if Utils.Strategy_print:
                                    print("Strategy.trade_task() 通过B最新tick判断B合约买挂单符合撤单条件")
                                # B合约撤单
                                order_action_arguments = {'OrderRef': i['OrderRef'].encode(),
                                                          'ExchangeID': i['ExchangeID'].encode(),
                                                          'OrderSysID': i['OrderSysID'].encode()}
                                if Utils.Strategy_print:
                                    print('Strategy.trade_task() B合约撤单，OrderRef=', i['OrderRef'], '撤单参数：', order_action_arguments)
                                self.__user.get_trade().OrderAction(order_action_arguments)
                        # B挂单的买卖方向为卖
                        elif i['Direction'] == '1':
                            # 挂单价格与盘口卖一价比较，如果与盘口价格差距n个最小跳以上，撤单
                            if Utils.Strategy_print:
                                print("Strategy.trade_task() self.__b_wait_price_tick * self.__b_price_tick", self.__b_wait_price_tick, self.__b_price_tick, type(self.__b_wait_price_tick), type(self.__b_price_tick))
                            if dict_arguments['tick']['AskPrice1'] <= (i['LimitPrice'] - self.__b_wait_price_tick * self.__b_price_tick):
                                if Utils.Strategy_print:
                                    print("Strategy.trade_task() 通过B最新tick判断B合约卖挂单符合撤单条件")
                                # B合约撤单
                                order_action_arguments = {'OrderRef': i['OrderRef'].encode(),
                                                          'ExchangeID': i['ExchangeID'].encode(),
                                                          'OrderSysID': i['OrderSysID'].encode()}
                                if Utils.Strategy_print:
                                    print('Strategy.trade_task() B合约撤单，OrderRef=', i['OrderRef'], '撤单参数：', order_action_arguments)
                                self.__user.get_trade().OrderAction(order_action_arguments)
            # 若无挂单、无撇退，则交易任务已完成
            # if True:
            #     self.__trade_tasking = False

    '''
    typedef char TThostFtdcOrderStatusType
    THOST_FTDC_OST_AllTraded = b'0'  # 全部成交
    THOST_FTDC_OST_PartTradedQueueing = b'1'  # 部分成交还在队列中
    THOST_FTDC_OST_PartTradedNotQueueing = b'2'  # 部分成交不在队列中
    THOST_FTDC_OST_NoTradeQueueing = b'3'  # 未成交还在队列中
    THOST_FTDC_OST_NoTradeNotQueueing = b'4'  # 未成交不在队列中
    THOST_FTDC_OST_Canceled = b'5'  # 撤单
    THOST_FTDC_OST_Unknown = b'a'  # 未知
    THOST_FTDC_OST_NotTouched = b'b'  # 尚未触发
    THOST_FTDC_OST_Touched = b'c'  # 已触发
    '''
    # 更新挂单列表
    def update_list_order_pending(self, dict_arguments):
        if Utils.Strategy_print:
            print("Strategy.update_list_order_pending() 更新前self.__list_order_pending=", self.__list_order_pending)
        # 交易所返回的报单回报，处理以上九种状态
        if len(dict_arguments['Order']['OrderSysID']) == 12:
            # 挂单列表为空时直接添加挂单到list中
            if len(self.__list_order_pending) == 0:
                self.__list_order_pending.append(dict_arguments['Order'])
                return
            # 挂单列表不为空时
            for i in range(len(self.__list_order_pending)):  # 遍历挂单列表
                # 找到回报与挂单列表中OrderRef相同的记录
                if self.__list_order_pending[i]['OrderRef'] == dict_arguments['Order']['OrderRef']:
                    if dict_arguments['Order']['OrderStatus'] == '0':  # 全部成交
                        self.__list_order_pending.remove(self.__list_order_pending[i])  # 将全部成交单从挂单列表删除
                    elif dict_arguments['Order']['OrderStatus'] == '1':  # 部分成交还在队列中
                        # i = dict_arguments['Order']  # 更新挂单列表
                        self.__list_order_pending[i] = dict_arguments['Order']  # 更新挂单列表
                        if Utils.Strategy_print:
                            print("Strategy.update_list_order_pending() 报单状态：部分成交还在队列中")
                    elif dict_arguments['Order']['OrderStatus'] == '2':  # 部分成交不在队列中
                        if Utils.Strategy_print:
                            print("Strategy.update_list_order_pending() 报单状态：部分成交不在队列中")
                    elif dict_arguments['Order']['OrderStatus'] == '3':  # 未成交还在队列中
                        # i = dict_arguments['Order']  # 更新挂单列表
                        self.__list_order_pending[i] = dict_arguments['Order']  # 更新挂单列表
                        if Utils.Strategy_print:
                            print("Strategy.update_list_order_pending() 报单状态：未成交还在队列中")
                    elif dict_arguments['Order']['OrderStatus'] == '4':  # 未成交不在队列中
                        if Utils.Strategy_print:
                            print("Strategy.update_list_order_pending() 报单状态：未成交不在队列中")
                    elif dict_arguments['Order']['OrderStatus'] == '5':  # 撤单
                        if Utils.Strategy_print:
                            print("Strategy.update_list_order_pending() 报单状态：撤单，合约：", dict_arguments['Order']['InstrumentID'])
                        self.__list_order_pending.remove(self.__list_order_pending[i])  # 将全部成交单从挂单列表删除
                    elif dict_arguments['Order']['OrderStatus'] == 'a':  # 未知
                        if Utils.Strategy_print:
                            print("Strategy.update_list_order_pending() 报单状态：未知")
                    elif dict_arguments['Order']['OrderStatus'] == 'b':  # 尚未触发
                        if Utils.Strategy_print:
                            print("Strategy.update_list_order_pending() 报单状态：尚未触发")
                    elif dict_arguments['Order']['OrderStatus'] == 'c':  # 已触发
                        if Utils.Strategy_print:
                            print("Strategy.update_list_order_pending() 报单状态：已触发")
                    if Utils.Strategy_print:
                        print("Strategy.update_list_order_pending() 更新后self.__list_order_pending=", self.__list_order_pending)
                    return
            # 挂单列表中找不到对应的OrderRef记录时，新添加挂单到self.__list_order_pending
            if dict_arguments['Order']['OrderStatus'] in ['1', '3']:
                self.__list_order_pending.append(dict_arguments['Order'])
                if Utils.Strategy_print:
                    print("Strategy.update_list_order_pending() 报单状态：部分成交还在队列中，未成交还在队列中")
        if Utils.Strategy_print:
            print("Strategy.update_list_order_pending() 更新后self.__list_order_pending=", self.__list_order_pending)

    # 更新任务状态
    def update_task_status(self):
        # if Utils.Strategy_print:
        #     print("Strategy.update_task_status() 更新前self.__trade_tasking=", self.__trade_tasking)
        if self.__position_a_buy_today == self.__position_b_sell_today \
                and self.__position_a_buy_yesterday == self.__position_b_sell_yesterday \
                and self.__position_a_sell_today == self.__position_b_buy_today \
                and self.__position_a_sell_yesterday == self.__position_b_buy_yesterday \
                and len(self.__list_order_pending) == 0:
            self.__trade_tasking = False
        else:
            self.__trade_tasking = True
        # if Utils.Strategy_print:
        #     print("Strategy.update_task_status() 更新后self.__trade_tasking=", self.__trade_tasking)

    """
    # 更新持仓量变量，共12个变量
    def update_position(self, dict_arguments):
        if Utils.Strategy_print:
            print("Strategy.update_position() 更新持仓量:")
        # A成交
        if dict_arguments['Order']['InstrumentID'] == self.__list_instrument_id[0]:
            if dict_arguments['Order']['CombOffsetFlag'] == '0':  # A开仓成交回报
                if dict_arguments['Order']['Direction'] == '0':  # A买开仓成交回报
                    self.__position_a_buy_today += dict_arguments['Order']['VolumeTraded']  # 更新持仓
                elif dict_arguments['Order']['Direction'] == '1':  # A卖开仓成交回报
                    self.__position_a_sell_today += dict_arguments['Order']['VolumeTraded']  # 更新持仓
            elif dict_arguments['Order']['CombOffsetFlag'] == '3':  # A平今成交回报
                if dict_arguments['Order']['Direction'] == '0':  # A买平今成交回报
                    self.__position_a_sell_today -= dict_arguments['Order']['VolumeTraded']  # 更新持仓
                elif dict_arguments['Order']['Direction'] == '1':  # A卖平今成交回报
                    self.__position_a_buy_today -= dict_arguments['Order']['VolumeTraded']  # 更新持仓
            elif dict_arguments['Order']['CombOffsetFlag'] == '4':  # A平昨成交回报
                if dict_arguments['Order']['Direction'] == '0':  # A买平昨成交回报
                    self.__position_a_sell_yesterday -= dict_arguments['Order']['VolumeTraded']  # 更新持仓
                elif dict_arguments['Order']['Direction'] == '1':  # A卖平昨成交回报
                    self.__position_a_buy_yesterday -= dict_arguments['Order']['VolumeTraded']  # 更新持仓
            self.__position_a_buy = self.__position_a_buy_today + self.__position_a_buy_yesterday
            self.__position_a_sell = self.__position_a_sell_today + self.__position_a_sell_yesterday
        # B成交
        elif dict_arguments['Order']['InstrumentID'] == self.__list_instrument_id[1]:
            if dict_arguments['Order']['CombOffsetFlag'] == '0':  # B开仓成交回报
                if dict_arguments['Order']['Direction'] == '0':  # B买开仓成交回报
                    self.__position_b_buy_today += dict_arguments['Order']['VolumeTraded']  # 更新持仓
                elif dict_arguments['Order']['Direction'] == '1':  # B卖开仓成交回报
                    self.__position_b_sell_today += dict_arguments['Order']['VolumeTraded']  # 更新持仓
            elif dict_arguments['Order']['CombOffsetFlag'] == '3':  # B平今成交回报
                if dict_arguments['Order']['Direction'] == '0':  # B买平今成交回报
                    self.__position_b_sell_today -= dict_arguments['Order']['VolumeTraded']  # 更新持仓
                elif dict_arguments['Order']['Direction'] == '1':  # B卖平今成交回报
                    self.__position_b_buy_today -= dict_arguments['Order']['VolumeTraded']  # 更新持仓
            elif dict_arguments['Order']['CombOffsetFlag'] == '4':  # B平昨成交回报
                if dict_arguments['Order']['Direction'] == '0':  # B买平昨成交回报
                    self.__position_b_sell_yesterday -= dict_arguments['Order']['VolumeTraded']  # 更新持仓
                elif dict_arguments['Order']['Direction'] == '1':  # B卖平昨成交回报
                    self.__position_b_buy_yesterday -= dict_arguments['Order']['VolumeTraded']  # 更新持仓
            self.__position_b_buy = self.__position_b_buy_today + self.__position_b_buy_yesterday
            self.__position_b_sell = self.__position_b_sell_today + self.__position_b_sell_yesterday
        if Utils.Strategy_print:
            print("     B合约：今买、昨买、总买", self.__position_b_buy_today, self.__position_b_buy_yesterday, self.__position_b_buy, "今卖、昨卖、总卖", self.__position_b_sell_today, self.__position_b_sell_yesterday, self.__position_b_sell)
        if Utils.Strategy_print:
            print("     A合约：今买、昨买、总买", self.__position_a_buy_today, self.__position_a_buy_yesterday, self.__position_a_buy, "今卖、昨卖、总卖", self.__position_a_sell_today, self.__position_a_sell_yesterday, self.__position_a_sell)
    """
    
    # 更新持仓量变量，共12个变量
    def update_position(self, Trade):
        if Utils.Strategy_print:
            print("Strategy.update_position() 更新持仓量:")
        # A成交
        if Trade['InstrumentID'] == self.__list_instrument_id[0]:
            if Trade['OffsetFlag'] == '0':  # A开仓成交回报
                if Trade['Direction'] == '0':  # A买开仓成交回报
                    self.__position_a_buy_today += Trade['Volume']  # 更新持仓
                elif Trade['Direction'] == '1':  # A卖开仓成交回报
                    self.__position_a_sell_today += Trade['Volume']  # 更新持仓
            elif Trade['OffsetFlag'] == '3':  # A平今成交回报
                if Trade['Direction'] == '0':  # A买平今成交回报
                    self.__position_a_sell_today -= Trade['Volume']  # 更新持仓
                elif Trade['Direction'] == '1':  # A卖平今成交回报
                    self.__position_a_buy_today -= Trade['Volume']  # 更新持仓
            elif Trade['OffsetFlag'] == '4':  # A平昨成交回报
                if Trade['Direction'] == '0':  # A买平昨成交回报
                    self.__position_a_sell_yesterday -= Trade['Volume']  # 更新持仓
                elif Trade['Direction'] == '1':  # A卖平昨成交回报
                    self.__position_a_buy_yesterday -= Trade['Volume']  # 更新持仓
            self.__position_a_buy = self.__position_a_buy_today + self.__position_a_buy_yesterday
            self.__position_a_sell = self.__position_a_sell_today + self.__position_a_sell_yesterday
        # B成交
        elif Trade['InstrumentID'] == self.__list_instrument_id[1]:
            if Trade['OffsetFlag'] == '0':  # B开仓成交回报
                if Trade['Direction'] == '0':  # B买开仓成交回报
                    self.__position_b_buy_today += Trade['Volume']  # 更新持仓
                elif Trade['Direction'] == '1':  # B卖开仓成交回报
                    self.__position_b_sell_today += Trade['Volume']  # 更新持仓
            elif Trade['OffsetFlag'] == '3':  # B平今成交回报
                if Trade['Direction'] == '0':  # B买平今成交回报
                    self.__position_b_sell_today -= Trade['Volume']  # 更新持仓
                elif Trade['Direction'] == '1':  # B卖平今成交回报
                    self.__position_b_buy_today -= Trade['Volume']  # 更新持仓
            elif Trade['OffsetFlag'] == '4':  # B平昨成交回报
                if Trade['Direction'] == '0':  # B买平昨成交回报
                    self.__position_b_sell_yesterday -= Trade['Volume']  # 更新持仓
                elif Trade['Direction'] == '1':  # B卖平昨成交回报
                    self.__position_b_buy_yesterday -= Trade['Volume']  # 更新持仓
            self.__position_b_buy = self.__position_b_buy_today + self.__position_b_buy_yesterday
            self.__position_b_sell = self.__position_b_sell_today + self.__position_b_sell_yesterday
        if Utils.Strategy_print:
            print("     A合约", self.__list_instrument_id[0], "今买、昨买、总买", self.__position_a_buy_today, self.__position_a_buy_yesterday,
                  self.__position_a_buy, "今卖、昨卖、总卖", self.__position_a_sell_today, self.__position_a_sell_yesterday,
                  self.__position_a_sell)
            print("     B合约", self.__list_instrument_id[1], "今买、昨买、总买", self.__position_b_buy_today, self.__position_b_buy_yesterday,
                  self.__position_b_buy, "今卖、昨卖、总卖", self.__position_b_sell_today, self.__position_b_sell_yesterday,
                  self.__position_b_sell)

    # 更新持仓明细list
    def update_list_position_detail(self, input_trade):
        trade = copy.deepcopy(input_trade)  # 形参深度拷贝到方法局部变量，目的是修改局部变量值不会影响到形参
        # 开仓单，添加到list，添加到list尾部
        if trade['OffsetFlag'] == '0':
            self.__list_position_detail.append(trade)
        # 平仓单，先开先平的原则从list里删除
        elif trade['OffsetFlag'] == '1':
            # 遍历self.__list_position_detail
            for i in range(len(self.__list_position_detail)):
                # trade中"OffsetFlag"为平今
                if trade['OffsetFlag'] == '3':
                    if self.__list_position_detail[i]['TradingDay'] == self.__TradingDay\
                            and trade['InstrumentID'] == self.__list_position_detail[i]['InstrumentID'] \
                            and trade['HedgeFlag'] == self.__list_position_detail[i]['HedgeFlag']\
                            and trade['Direction'] != self.__list_position_detail[i]['Direction']:
                            # list_position_detail中的交易日是当前交易日
                            # trade和list_position_detail中的合约代码相同
                            # trade和list_position_detail中的投保标志相同
                            # trade和list_position_detail中的买卖不同
                        # trade中的volume等于持仓明细列表中的volume
                        if trade['Volume'] == self.__list_position_detail[i]['Volume']:
                            del self.__list_position_detail[i]
                        # trade中的volume小于持仓明细列表中的volume
                        elif trade['Volume'] < self.__list_position_detail[i]['Volume']:
                            self.__list_position_detail[i]['Volume'] -= trade['Volume']
                        # trade中的volume大于持仓明细列表中的volume
                        elif trade['Volume'] > self.__list_position_detail[i]['Volume']:
                            trade['Volume'] -= self.__list_position_detail[i]['Volume']
                            del self.__list_position_detail[i]
                # trade中"OffsetFlag"为平昨
                elif trade['OffsetFlag'] == '4':
                    if self.__list_position_detail[i]['TradingDay'] != self.__TradingDay \
                            and trade['InstrumentID'] == self.__list_position_detail[i]['InstrumentID'] \
                            and trade['HedgeFlag'] == self.__list_position_detail[i]['HedgeFlag'] \
                            and trade['Direction'] != self.__list_position_detail[i]['Direction']:
                        # list_position_detail中的交易日不是当前交易日
                        # trade和list_position_detail中的合约代码相同
                        # trade和list_position_detail中的投保标志相同
                        # trade和list_position_detail中的买卖不同
                        # trade中的volume等于持仓明细列表中的volume
                        if trade['Volume'] == self.__list_position_detail[i]['Volume']:
                            del self.__list_position_detail[i]
                        # trade中的volume小于持仓明细列表中的volume
                        elif trade['Volume'] < self.__list_position_detail[i]['Volume']:
                            self.__list_position_detail[i]['Volume'] -= trade['Volume']
                        # trade中的volume大于持仓明细列表中的volume
                        elif trade['Volume'] > self.__list_position_detail[i]['Volume']:
                            trade['Volume'] -= self.__list_position_detail[i]['Volume']
                            del self.__list_position_detail[i]

