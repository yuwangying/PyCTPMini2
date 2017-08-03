# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 00:31:14 2016

@author: Zhuolin
"""

import sys
from datetime import datetime
import threading
import pandas as pd
from pandas import Series, DataFrame
import Utils
import pyctp
import copy
# from MessageBox import MessageBox


class PyCTP_Trader_API(pyctp.CThostFtdcTraderSpi):

    TIMEOUT = 300

    __RequestID = 0
    __isLogined = False
    # __df_order = DataFrame()  # 保存该期货账户的所有OnRtnOrder来的记录
    # __df_trade = DataFrame()  # 保存该期货账户的所有OnRtnTrade来的记录
    # __df_qry_order = DataFrame()  # 保存该期货账户的所有QryOrder返回的记录
    # __df_qry_trade = DataFrame()  # 保存该期货账户的所有QryTrade返回的记录

    def __init__(self, dict_args):
        pyctp.CThostFtdcTraderSpi.__init__(self)
        # print(">>>PyCTP_Trader.__init__() dict_args =", dict_args)
        self.__front_address = dict_args['frontaddress']
        self.__broker_id = dict_args['brokerid']
        self.__user_id = dict_args['userid']
        self.__password = dict_args['password']

    def __IncRequestID(self):
        """ 自增并返回请求ID """
        self.__RequestID += 1
        return self.__RequestID

    # ywy新增加
    def __IncOrderRef(self):
        """ 递增报单引用 """
        OrderRef = bytes('%012d' % self.__OrderRef, 'gb2312')
        self.__OrderRef += 1
        return OrderRef

    def __IncOrderActionRef(self):
        """ 递增报单操作引用 """
        OrderActionRef = bytes('%012d' % self.__OrderActionRef, 'gb2312')
        self.__OrderActionRef += 1
        return OrderActionRef

    def setInvestorID(self, InvestorID):
        self.__InvestorID = InvestorID

    def Connect(self, front_address, model):
        """ 连接前置服务器 """
        # self.RegisterSpi(self)
        # self.SubscribePrivateTopic(model)  # 从本次连线之后开始发送数据
        # self.SubscribePublicTopic(PyCTP.THOST_TERT_QUICK)
        # self.RegisterFront(front_address.encode())
        # self.Init()
        # 创建api对象
        file_path = 'conn/td/' + self.__user_id + '/'
        self.api = pyctp.CThostFtdcTraderApi_CreateFtdcTraderApi(file_path)
        self.__user.set_api(self.api)
        self.api.SubscribePrivateTopic(pyctp.THOST_TERT_RESTART)
        self.api.SubscribePublicTopic(pyctp.THOST_TERT_RESTART)
        self.api.RegisterSpi(self)
        self.api.RegisterFront(front_address)
        self.api.Init()

        self.__rsp_Connect = dict(event=threading.Event())
        self.__rsp_Connect['event'].clear()
        return 0 if self.__rsp_Connect['event'].wait(self.TIMEOUT) else -4

    def UnConnect(self):
        """ywy：断开连接，释放TradeApi"""
        self.api.RegisterSpi(None)
        self.api.Release()

    def Login(self, BrokerID, UserID, Password):
        """ 用户登录请求 """
        # reqUserLogin = dict(BrokerID=BrokerID,
        #                     UserID=UserID,
        #                     Password=Password)
        reqUserLoginField = pyctp.CThostFtdcReqUserLoginField()
        reqUserLoginField.Password = Password
        reqUserLoginField.UserID = UserID
        reqUserLoginField.BrokerID = BrokerID
        self.__rsp_Login = dict(event=threading.Event(),
                                RequestID=self.__IncRequestID())
        # print(">>>PyCTP_Trade.Login() self.api.ReqUserLogin() called")
        ret = self.api.ReqUserLogin(reqUserLoginField, self.__rsp_Login['RequestID'])
        if ret == 0:
            self.__rsp_Login['event'].clear()
            if self.__rsp_Login['event'].wait(self.TIMEOUT):
                if self.__rsp_Login['ErrorID'] == 0:
                    self.__isLogined = True
                    self.__Password = Password
                else:
                    sys.stderr.write(str(self.__rsp_Login['ErrorMsg'], encoding='gb2312'))
                return self.__rsp_Login['ErrorID']
            else:
                return -4
        return ret

    # 获取登录状态
    def get_isLogined(self):
        return self.__isLogined

    def Logout(self):
        """ 登出请求 """
        # reqUserLogout = dict(BrokerID=self.__BrokerID, UserID=self.__UserID)
        feild_reqUserLogout = pyctp.CThostFtdcUserLogoutField()
        feild_reqUserLogout.BrokerID = self.__BrokerID
        feild_reqUserLogout.BrokerID = self.__UserID
        self.__rsp_Logout = dict(event=threading.Event(), RequestID=self.__IncRequestID())
        ret = self.api.ReqUserLogout(feild_reqUserLogout, self.__rsp_Logout['RequestID'])
        if ret == 0:
            self.__rsp_Logout['event'].clear()
            if self.__rsp_Logout['event'].wait(self.TIMEOUT):
                if self.__rsp_Logout['ErrorID'] == 0:
                    self.__isLogined = False
                return self.__rsp_Logout['ErrorID']
            else:
                return -4
        return ret

    def QryInstrument(self, ExchangeID='', InstrumentID=''):
        """ 请求查询合约 """
        # QryInstrument = dict(ExchangeID=ExchangeID, InstrumentID=InstrumentID)
        feild_QryInstrument = pyctp.CThostFtdcQryInstrumentField()
        feild_QryInstrument.InstrumentID = InstrumentID
        feild_QryInstrument.ExchangeID = ExchangeID
        self.__rsp_QryInstrument = dict(event=threading.Event(),
                                        RequestID=self.__IncRequestID(),
                                        results=[],
                                        ErrorID=0)
        ret = self.api.ReqQryInstrument(feild_QryInstrument, self.__rsp_QryInstrument['RequestID'])
        if ret == 0:
            # if self.__rsp_QryInstrument['ErrorID'] != 0:
            #     return self.__rsp_QryInstrument['ErrorID']
            # else:
            #     return self.__rsp_QryInstrument['results']
            self.__rsp_QryInstrument['event'].clear()
            if self.__rsp_QryInstrument['event'].wait(60.0):
                if self.__rsp_QryInstrument['ErrorID'] != 0:
                    return self.__rsp_QryInstrument['ErrorID']
                return self.__rsp_QryInstrument['results']
            else:
                print('PyCTP_Trade_API.QryInstrument() -4')
                return -4
        return ret

    def QryInstrumentMarginRate(self, InstrumentID):
        """ 请求查询合约保证金率 """
        # QryInstrumentMarginRate = dict(BrokerID=self.__BrokerID,
        #                                InvestorID=self.__InvestorID,
        #                                InstrumentID=InstrumentID)
        feild_QryInstrumentMarginRate = pyctp.CThostFtdcQryInstrumentMarginRateField()
        feild_QryInstrumentMarginRate.BrokerID = self.__BrokerID
        feild_QryInstrumentMarginRate.InvestorID = self.__InvestorID
        feild_QryInstrumentMarginRate.InstrumentID = InstrumentID
        self.__rsp_QryInstrumentMarginRate = dict(results=[],
                                                  RequestID=self.__IncRequestID(),
                                                  ErrorID=0,
                                                  event=threading.Event())
        ret = self.api.ReqQryInstrumentMarginRate(feild_QryInstrumentMarginRate, self.__rsp_QryInstrumentMarginRate['RequestID'])
        if ret == 0:
            self.__rsp_QryInstrumentMarginRate['event'].clear()
            if self.__rsp_QryInstrumentMarginRate['event'].wait(self.TIMEOUT):
                if self.__rsp_QryInstrumentMarginRate['ErrorID'] != 0:
                    return self.__rsp_QryInstrumentMarginRate['ErrorID']
                return self.__rsp_QryInstrumentMarginRate['results']
            else:
                return -4
        return ret

    def QryInstrumentCommissionRate(self, InstrumentID):
        """ 请求查询合约手续费率 """
        # QryInstrumentCommissionRate = dict(BrokerID=self.__BrokerID, InvestorID=self.__InvestorID, InstrumentID=InstrumentID)
        feild_QryInstrumentCommissionRate = pyctp.CThostFtdcQryInstrumentCommissionRateField()
        feild_QryInstrumentCommissionRate.BrokerID = self.__BrokerID
        feild_QryInstrumentCommissionRate.InvestorID = self.__InvestorID
        feild_QryInstrumentCommissionRate.InstrumentID = InstrumentID
        self.__rsp_QryInstrumentCommissionRate = dict(results=[],
                                                      RequestID=self.__IncRequestID(),
                                                      ErrorID=0,
                                                      event=threading.Event())
        ret = self.api.ReqQryInstrumentCommissionRate(feild_QryInstrumentCommissionRate, self.__rsp_QryInstrumentCommissionRate['RequestID'])
        if ret == 0:
            self.__rsp_QryInstrumentCommissionRate['event'].clear()
            if self.__rsp_QryInstrumentCommissionRate['event'].wait(self.TIMEOUT):
                if self.__rsp_QryInstrumentCommissionRate['ErrorID'] != 0:
                    return self.__rsp_QryInstrumentCommissionRate['ErrorID']
                return self.__rsp_QryInstrumentCommissionRate['results']
            else:
                return -4
        return ret

    def QryOrder(self, InstrumentID='', OrderSysID='', InsertTimeStart='', InsertTimeEnd=''):
        """请求查询报单"""
        # QryOrderField = dict(BrokerID=self.__BrokerID, InvestorID=self.__InvestorID, OrderSysID=OrderSysID, InsertTimeStart=InsertTimeStart, InsertTimeEnd=InsertTimeEnd, InstrumentID=InstrumentID)
        feild_QryOrderField = pyctp.CThostFtdcQryOrderField()
        feild_QryOrderField.BrokerID = self.__BrokerID
        feild_QryOrderField.InvestorID = self.__InvestorID
        feild_QryOrderField.InstrumentID = InstrumentID
        feild_QryOrderField.OrderSysID = OrderSysID
        feild_QryOrderField.InsertTimeStart = InsertTimeStart
        feild_QryOrderField.InsertTimeEnd = InsertTimeEnd
        self.__rsp_QryOrder = dict(results=[], RequestID=self.__IncRequestID(), ErrorID=0, event=threading.Event())
        ret = self.api.ReqQryOrder(feild_QryOrderField, self.__rsp_QryOrder['RequestID'])
        if ret == 0:
            self.__rsp_QryOrder['event'].clear()
            if self.__rsp_QryOrder['event'].wait(60.0):
                if self.__rsp_QryOrder['ErrorID'] != 0:
                    # self.__user.QryOrder(self.__rsp_QryOrder['ErrorID'])  # 转到user类的回调函数
                    return self.__rsp_QryOrder['ErrorID']
                # self.__user.QryOrder(self.__rsp_QryOrder['results'])  # 转到user类的回调函数
                return Utils.code_transform(self.__rsp_QryOrder['results'])
            else:
                # self.__user.QryOrder(-4)  # 转到user类的回调函数
                print(" PyCTP_Trade.QryTrade() user_id=", self.__UserID, "请求查询报单异常，-4")
                return -4
        # self.__user.QryOrder(ret)  # 转到user类的回调函数
        return ret

    def QryTrade(self, InstrumentID='', ExchangeID='', TradeID=''):
        """请求查询成交单"""
        QryTradeField = dict(BrokerID=self.__BrokerID, InvestorID=self.__InvestorID, InstrumentID=InstrumentID, ExchangeID=ExchangeID, TradeID=TradeID)
        feild_QryTradeField = pyctp.CThostFtdcQryTradeField()
        feild_QryTradeField.BrokerID = self.__BrokerID
        feild_QryTradeField.InvestorID = self.__InvestorID
        feild_QryTradeField.InstrumentID = InstrumentID
        feild_QryTradeField.ExchangeID = ExchangeID
        feild_QryTradeField.TradeID = TradeID
        self.__rsp_QryTrade = dict(results=[], RequestID=self.__IncRequestID(), ErrorID=0, event=threading.Event())
        ret = self.api.ReqQryTrade(feild_QryTradeField, self.__rsp_QryTrade['RequestID'])
        if ret == 0:
            self.__rsp_QryTrade['event'].clear()
            if self.__rsp_QryTrade['event'].wait(60.0):  # (self.TIMEOUT):  # 成交记录多，传输等待时间需要延长
                if self.__rsp_QryTrade['ErrorID'] != 0:
                    # self.__user.QryTrade(self.__rsp_QryTrade['ErrorID'])  # 转到user类的回调函数
                    return self.__rsp_QryTrade['ErrorID']
                # self.__user.QryTrade(self.__rsp_QryTrade['results'])  # 转到user类的回调函数
                return self.__rsp_QryTrade['results']
            else:
                # self.__user.QryTrade(-4)  # 转到user类的回调函数
                print(" PyCTP_Trade.QryTrade() user_id=", self.__UserID, "请求查询成交单异常，-4")
                return -4
        # self.__user.QryTrade(ret)  # 转到user类的回调函数
        return ret

    def QryInvestorPosition(self, InstrumentID=''):
        """ 请求查询投资者持仓 """
        # QryInvestorPositionField = dict(BrokerID=self.__BrokerID, InvestorID=self.__InvestorID, InstrumentID=InstrumentID)
        QryInvestorPositionField = pyctp.CThostFtdcQryInvestorPositionField()
        QryInvestorPositionField.BrokerID = self.__BrokerID
        QryInvestorPositionField.InvestorID = self.__InvestorID
        QryInvestorPositionField.InstrumentID = InstrumentID
        self.__rsp_QryInvestorPosition = dict(results=[], RequestID=self.__IncRequestID(), ErrorID=0, event=threading.Event())
        ret = self.api.ReqQryInvestorPosition(QryInvestorPositionField, self.__rsp_QryInvestorPosition['RequestID'])
        if ret == 0:
            self.__rsp_QryInvestorPosition['event'].clear()
            if self.__rsp_QryInvestorPosition['event'].wait(self.TIMEOUT):
                if self.__rsp_QryInvestorPosition['ErrorID'] != 0:
                    return self.__rsp_QryInvestorPosition['ErrorID']
                return self.__rsp_QryInvestorPosition['results']
            else:
                return -4
        return ret

    def QryInvestorPositionDetail(self, InstrumentID=''):
        """ 请求查询投资者持仓明细 """
        # ywy 20170120
        # QryInvestorPositionFieldDetail = dict(BrokerID=self.__BrokerID,
        #                                       InvestorID=self.__InvestorID,
        #                                       InstrumentID=InstrumentID)
        QryInvestorPositionDetailField = pyctp.CThostFtdcQryInvestorPositionDetailField()
        QryInvestorPositionDetailField.BrokerID = self.__BrokerID
        QryInvestorPositionDetailField.InvestorID = self.__InvestorID
        QryInvestorPositionDetailField.InstrumentID = InstrumentID
        self.__rsp_QryInvestorPositionDetail = dict(results=[],
                                                    RequestID=self.__IncRequestID(),
                                                    ErrorID=0,
                                                    event=threading.Event())
        ret = self.api.ReqQryInvestorPositionDetail(QryInvestorPositionDetailField, self.__rsp_QryInvestorPositionDetail['RequestID'])
        if ret == 0:
            self.__rsp_QryInvestorPositionDetail['event'].clear()
            if self.__rsp_QryInvestorPositionDetail['event'].wait(self.TIMEOUT):
                if self.__rsp_QryInvestorPositionDetail['ErrorID'] != 0:
                    return self.__rsp_QryInvestorPositionDetail['ErrorID']
                return self.__rsp_QryInvestorPositionDetail['results']
            else:
                return -4
        return ret

    def QryTradingAccount(self):
        """ 请求查询资金账户 """
        # QryTradingAccountField = dict(BrokerID=self.__BrokerID, InvestorID=self.__InvestorID)
        QryTradingAccountField = pyctp.CThostFtdcQryTradingAccountField()
        QryTradingAccountField.BrokerID = self.__BrokerID
        QryTradingAccountField.InvestorID = self.__InvestorID
        self.__rsp_QryTradingAccount = dict(results=[], RequestID=self.__IncRequestID(), ErrorID=0, event=threading.Event())
        ret = self.api.ReqQryTradingAccount(QryTradingAccountField, self.__rsp_QryTradingAccount['RequestID'])
        if ret == 0:
            self.__rsp_QryTradingAccount['event'].clear()
            if self.__rsp_QryTradingAccount['event'].wait(self.TIMEOUT):
                if self.__rsp_QryTradingAccount['ErrorID'] != 0:
                    return self.__rsp_QryTradingAccount['ErrorID']
                return self.__rsp_QryTradingAccount['results']
            else:
                return -4
        return ret

    def QryInvestor(self):
        """ 请求查询投资者 """
        # InvestorField = dict(BrokerID=self.__BrokerID, InvestorID=self.__InvestorID)
        QryInvestorField = pyctp.CThostFtdcQryInvestorField()
        QryInvestorField.BrokerID = self.__BrokerID
        QryInvestorField.InvestorID = self.__InvestorID
        self.__rsp_QryInvestor = dict(results=[], RequestID=self.__IncRequestID(), ErrorID=0, event=threading.Event())
        ret = self.api.ReqQryInvestor(QryInvestorField, self.__rsp_QryInvestor['RequestID'])
        if ret == 0:
            self.__rsp_QryInvestor['event'].clear()
            if self.__rsp_QryInvestor['event'].wait(self.TIMEOUT):
                if self.__rsp_QryInvestor['ErrorID'] != 0:
                    return self.__rsp_QryInvestor['ErrorID']
                return self.__rsp_QryInvestor['results']
            else:
                return -4
        return ret

    def QryTradingCode(self, exchangeid):
        """请求查询交易编码"""
        # QryTradingCodeField = dict(BrokerID=self.__BrokerID, InvestorID=self.__InvestorID,
        #                            ExchangeID=exchangeid)
        QryTradingCodeField = pyctp.CThostFtdcQryTradingCodeField()
        QryTradingCodeField.BrokerID = self.__BrokerID
        QryTradingCodeField.InvestorID = self.__InvestorID
        QryTradingCodeField.ExchangeID = exchangeid
        self.__rsp_QryTradingCode = dict(results=[], RequestID=self.__IncRequestID(), ErrorID=0, event=threading.Event())
        ret = self.api.ReqQryTradingCode(QryTradingCodeField, self.__rsp_QryTradingCode['RequestID'])
        if ret == 0:
            self.__rsp_QryTradingCode['event'].clear()  # set event flag False
            if self.__rsp_QryTradingCode['event'].wait(self.TIMEOUT):
                if self.__rsp_QryTradingCode['ErrorID'] != 0:
                    return self.__rsp_QryTradingCode['ErrorID']
                return self.__rsp_QryTradingCode['results']
            else:
                return -4
        return ret

    def OnRtnInstrumentStatus(self, InstrumentStatus):
        pass

    def QryExchange(self, ExchangeID=''):
        """ 请求查询交易所 """
        # QryExchangeField = dict(ExchangeID=ExchangeID)
        QryExchangeField = pyctp.CThostFtdcQryExchangeField()
        QryExchangeField.ExchangeID = ExchangeID
        self.__rsp_QryExchange = dict(results=[], RequestID=self.__IncRequestID(), ErrorID=0, event=threading.Event())
        ret = self.api.ReqQryExchange(QryExchangeField, self.__rsp_QryExchange['RequestID'])
        if ret == 0:
            self.__rsp_QryExchange['event'].clear()
            if self.__rsp_QryExchange['event'].wait(self.TIMEOUT):
                if self.__rsp_QryExchange['ErrorID'] != 0:
                    return self.__rsp_QryExchange['ErrorID']
                return self.__rsp_QryExchange['results']
            else:
                return -4
        return ret

    def QryDepthMarketData(self, InstrumentID):
        """ 请求查询行情 """
        # QryDepthMarketData = dict(InstrumentID=InstrumentID)
        QryDepthMarketData = pyctp.CThostFtdcQryDepthMarketDataField()
        QryDepthMarketData.InstrumentID = InstrumentID
        self.__rsp_QryDepthMarketData = dict(results=[], RequestID=self.__IncRequestID(), ErrorID=0, event=threading.Event())
        ret = self.api.ReqQryDepthMarketData(QryDepthMarketData, self.__rsp_QryDepthMarketData['RequestID'])
        if ret == 0:
            self.__rsp_QryDepthMarketData['event'].clear()
            if self.__rsp_QryDepthMarketData['event'].wait(self.TIMEOUT):
                if self.__rsp_QryDepthMarketData['ErrorID'] != 0:
                    return self.__rsp_QryDepthMarketData['ErrorID']
                return self.__rsp_QryDepthMarketData['results']
            else:
                return -4
        return ret

    def OrderInsert(self, dict_arguments):
        # InstrumentID, CombOffsetFlag, Direction, VolumeTotalOriginal, LimitPrice, OrderRef):
        print(">>> PyCTP_Trade.OrderInser() dict_arguments =", dict_arguments)
        """报单录入请求:开平仓(限价挂单)申报"""
        # InputOrder = {'BrokerID': self.__BrokerID,  # 经纪公司代码
        #               'InvestorID': self.__InvestorID,  # 投资者代码
        #               'UserID': self.__UserID,  # 用户代码
        #               'OrderPriceType': pyctp.THOST_FTDC_OPT_LimitPrice,  # 报单价格条件:限价
        #               'TimeCondition': pyctp.THOST_FTDC_TC_GFD,  # 有效期类型:当日有效
        #               'VolumeCondition': pyctp.THOST_FTDC_VC_AV,  # 成交量类型:任意数量
        #               'ContingentCondition': pyctp.THOST_FTDC_CC_Immediately,  # 触发条件:立即
        #               'ForceCloseReason': pyctp.THOST_FTDC_FCC_NotForceClose,  # 强平原因:非强平
        #               'MinVolume': 1,  # 最小成交量
        #               'InstrumentID': dict_arguments['InstrumentID'],  # 合约代码
        #               'LimitPrice': dict_arguments['LimitPrice'],  # 价格
        #               'VolumeTotalOriginal': dict_arguments['VolumeTotalOriginal'],  # 数量
        #               'Direction': dict_arguments['Direction'],  # 方向
        #               'CombOffsetFlag': dict_arguments['CombOffsetFlag'],  # 组合开平标志，上期所3平今、4平昨，其他交易所1平仓
        #               'CombHedgeFlag': dict_arguments['CombHedgeFlag'],  # 组合投机套保标志:投机
        #               'OrderRef': dict_arguments['OrderRef'],  # 报单引用
        #               }
        InputOrderField = pyctp.CThostFtdcInputOrderField()
        InputOrderField.BrokerID = self.__BrokerID
        InputOrderField.InvestorID = self.__InvestorID
        InputOrderField.UserID = self.__UserID
        InputOrderField.OrderPriceType = pyctp.THOST_FTDC_OPT_LimitPrice
        InputOrderField.TimeCondition = pyctp.THOST_FTDC_TC_GFD
        InputOrderField.VolumeCondition = pyctp.THOST_FTDC_VC_AV
        InputOrderField.ContingentCondition = pyctp.THOST_FTDC_CC_Immediately
        InputOrderField.ForceCloseReason = pyctp.THOST_FTDC_FCC_NotForceClose
        InputOrderField.MinVolume = 1
        InputOrderField.InstrumentID = dict_arguments['InstrumentID']
        InputOrderField.LimitPrice = dict_arguments['LimitPrice']
        InputOrderField.VolumeTotalOriginal = dict_arguments['VolumeTotalOriginal']
        InputOrderField.Direction = dict_arguments['Direction']
        InputOrderField.CombOffsetFlag = dict_arguments['CombOffsetFlag']
        InputOrderField.CombHedgeFlag = dict_arguments['CombHedgeFlag']
        InputOrderField.OrderRef = dict_arguments['OrderRef']
        self.__rsp_OrderInsert = dict(FrontID=self.__FrontID,
                                      SessionID=self.__SessionID,
                                      InputOrder=InputOrderField,
                                      RequestID=self.__IncRequestID(),
                                      event=threading.Event())
        ret = self.api.ReqOrderInsert(InputOrderField, self.__rsp_OrderInsert['RequestID'])
        if ret == 0:
            # self.__rsp_OrderInsert['event'].clear()
            if True:  # self.__rsp_OrderInsert['event'].wait(self.TIMEOUT):
                if 'ErrorID' in self.__rsp_OrderInsert.keys():
                    if self.__rsp_OrderInsert['ErrorID'] != 0:
                        # print("PyCTP_Trade.OrderInsert()", self.__rsp_OrderInsert['ErrorMsg'])
                        # sys.stderr.write(str(self.__rsp_OrderInsert['ErrorMsg'], encoding='gb2312'))  # 错误代码，用上一行代替
                        return self.__rsp_OrderInsert['ErrorID']
                return self.__rsp_OrderInsert.copy()
            else:
                return -4
        return ret

    def OrderInsertDict(self, InstrumentID):
        """报单录入请求:开平仓(限价挂单)申报"""
        # ctpmini2版本的小蜜蜂客户端无任何使用到该方法的地方，未在ctp标准版代码基础上做修改，不可用
        InputOrder = {}
        InputOrder.update(InstrumentID)

        self.__rsp_OrderInsert = dict(FrontID=self.__FrontID
                                      , SessionID=self.__SessionID
                                      , InputOrder=InputOrder
                                      , RequestID=self.__IncRequestID()
                                      , event=threading.Event())
        print('ReqOrderInsert的传入参数', InputOrder)
        ret = self.ReqOrderInsert(InputOrder, self.__rsp_OrderInsert['RequestID'])
        if ret == 0:
            # self.__rsp_OrderInsert['event'].clear()
            if True:  # self.__rsp_OrderInsert['event'].wait(self.TIMEOUT):
                if self.__rsp_OrderInsert['ErrorID'] != 0:
                    sys.stderr.write(str(self.__rsp_OrderInsert['ErrorMsg'], encoding='gb2312'))
                    return self.__rsp_OrderInsert['ErrorID']
                return self.__rsp_OrderInsert.copy()
            else:
                return -4
        return ret

    def OrderAction(self, dict_arguments):
        # ExchangeID, OrderRef, OrderSysID):
        """报单操作请求"""
        """ 报单操作请求(撤单), 注意,这是异步指令 """
        InputOrderAction = {'BrokerID': self.__BrokerID,  # 经纪公司代码
                            'UserID': self.__UserID,  # 用户代码
                            'InvestorID': self.__InvestorID,  # 投资者代码
                            'RequestID': self.__IncRequestID(),  # 请求编号
                            'ActionFlag': pyctp.THOST_FTDC_AF_Delete,  # 操作标志:撤单
                            'OrderActionRef': int(self.__IncOrderActionRef()),  # 报单操作引用
                            'OrderRef': dict_arguments['OrderRef'],  # 报单引用（未成交挂单的报单引用）
                            'ExchangeID': dict_arguments['ExchangeID'],  # 交易所代码
                            'OrderSysID': dict_arguments['OrderSysID'],  # 报单编号（未成交挂单的报单编号）
                            }
        InputOrderActionField = pyctp.CThostFtdcInputOrderActionField()
        InputOrderActionField.BrokerID = self.__BrokerID
        InputOrderActionField.UserID = self.__UserID
        InputOrderActionField.InvestorID = self.__InvestorID
        InputOrderActionField.RequestID = self.__IncRequestID()
        InputOrderActionField.ActionFlag = pyctp.THOST_FTDC_AF_Delete
        InputOrderActionField.OrderActionRef = int(self.__IncOrderActionRef())
        InputOrderActionField.OrderRef = dict_arguments['OrderRef']
        InputOrderActionField.ExchangeID = dict_arguments['ExchangeID']
        InputOrderActionField.OrderSysID = dict_arguments['OrderSysID']
        self.__rsp_OrderAction = dict(FrontID=self.__FrontID,  # 前置编号
                                      SessionID=self.__SessionID,  # 会话编号
                                      InputOrderAction=InputOrderActionField,
                                      event=threading.Event())
        ret = self.ReqOrderAction(InputOrderActionField, InputOrderActionField.RequestID)
        if ret == 0:
            # self.__rsp_OrderAction['event'].clear()
            if True:  # self.__rsp_OrderAction['event'].wait(self.TIMEOUT):
                print("PyCTP_Trade.OrderAction().self.__rsp_OrderAction=", self.__rsp_OrderAction)
                if 'ErrorID' in self.__rsp_OrderAction:
                    if self.__rsp_OrderAction['ErrorID'] != 0:
                        sys.stderr.write(str(self.__rsp_OrderInsert['ErrorMsg'], encoding='gb2312'))
                        return self.__rsp_OrderAction['ErrorID']
                    return self.__rsp_OrderAction.copy()
                else:
                    print("PyCTP_Trade.OrderAction() ErrorID is not in self.__rsp_OrderAction")
            else:
                return -4
        return ret
        pass

    # def OrderActionDelete(self, order):
    #     """ 撤销报单 """
    #     # OrderAction的再封装
    #     return super().OrderAction(PyCTP.THOST_FTDC_AF_Delete, order['FrontID'], order['SessionID'], order['OrderRef'],
    #                                order['ExchangeID'], order['OrderSysID'])
    def OnFrontConnected(self):
        """ 当客户端与交易后台建立起通信连接时（还未登录前），该方法被调用。 """
        self.__rsp_Connect['event'].set()

    def OnFrontDisconnected(self, nReason):
        """ 当客户端与交易后台通信连接断开时，该方法被调用。当发生这个情况后，API会自动重新连接，客户端可不做处理。
        nReason 错误原因
        0x1001 网络读失败
        0x1002 网络写失败
        0x2001 接收心跳超时
        0x2002 发送心跳失败
        0x2003 收到错误报文
        """
        sys.stderr.write('前置连接中断: %s' % hex(nReason))
        # 登陆状态时掉线, 自动重登陆
        # if self.__isLogined:
        # self.__Inst_Interval()
        # sys.stderr.write('自动登陆: %d' % self.Login(self.__BrokerID, self.__UserID, self.__Password))

    def OnRspUserLogin(self, RspUserLoginField, RspInfoField, RequestID, IsLast):
        """ 登录请求响应 """
        if RequestID == self.__rsp_Login['RequestID'] and IsLast:
            self.__BrokerID = RspUserLoginField.BrokerID
            self.__InvestorID = RspUserLoginField.UserID
            self.__SystemName = RspUserLoginField.SystemName
            self.__TradingDay = RspUserLoginField.TradingDay
            self.__SessionID = RspUserLoginField.SessionID
            self.__MaxOrderRef = RspUserLoginField.MaxOrderRef
            self.__OrderRef = 0  # 初始化报单引用
            self.__OrderActionRef = 0
            self.__LoginTime = RspUserLoginField.LoginTime
            self.__FrontID = RspUserLoginField.FrontID
            self.__FFEXTime = RspUserLoginField.FFEXTime
            self.__CZCETime = RspUserLoginField.CZCETime
            self.__SHFETime = RspUserLoginField.SHFETime
            self.__DCETime = RspUserLoginField.DCETime
            self.__INETime = RspUserLoginField.INETime
            RspInfo = {
                'ErrorID': RspInfoField.ErrorID,  # 错误代码
                'ErrorMsg': RspInfoField.ErrorMsg  # 错误信息
            }
            print(">>>PyCTP_Trade.OnRspUserLogin()  RspInfo =", RspInfo)
            self.__rsp_Login.update(RspInfo)
            self.__rsp_Login['event'].set()

    def OnRspUserLogout(self, RspUserLogout, RspInfo, RequestID, IsLast):
        """ 登出请求响应 """
        if RequestID == self.__rsp_Logout['RequestID'] and IsLast:
            self.__rsp_Logout.update(RspInfo)
            self.__rsp_Logout['event'].set()

    def OnRspQryInstrument(self, InstrumentField, RspInfoField, RequestID, IsLast):
        """ 请求查询合约响应 """
        if RequestID == self.__rsp_QryInstrument['RequestID']:
            if RspInfoField is not None:
                RspInfo = {
                    'ErrorID': RspInfoField.ErrorID,  # 错误代码
                    'ErrorMsg': RspInfoField.ErrorMsg  # 错误信息
                }
                self.__rsp_QryInstrument.update(RspInfo)
            if InstrumentField is not None:
                Instrument = {
                    'InstrumentID': InstrumentField.InstrumentID,  # 合约代码;
                    'ExchangeID': InstrumentField.ExchangeID,  # 交易所代码
                    'InstrumentName': InstrumentField.InstrumentName,  # 合约名称
                    'ExchangeInstID': InstrumentField.ExchangeInstID,  # 合约在交易所的代码
                    'ProductID': InstrumentField.ProductID,  # 产品代码
                    'ProductClass': InstrumentField.ProductClass,  # 产品类型
                    'DeliveryYear': InstrumentField.DeliveryYear,  # 交割年份
                    'DeliveryMonth': InstrumentField.DeliveryMonth, # 交割月
                    'MaxMarketOrderVolume': InstrumentField.MaxMarketOrderVolume,  # 市价单最大下单量
                    'MinMarketOrderVolume': InstrumentField.MinMarketOrderVolume,  # 市价单最小下单量
                    'MaxLimitOrderVolume': InstrumentField.MaxLimitOrderVolume,  # 限价单最大下单量
                    'MinLimitOrderVolume': InstrumentField.MinLimitOrderVolume,  # 限价单最小下单量
                    'VolumeMultiple': InstrumentField.VolumeMultiple,  # 合约数量乘数
                    'PriceTick': InstrumentField.PriceTick,  # 最小变动价位
                    'CreateDate':  InstrumentField.CreateDate,  # 创建日
                    'OpenDate': InstrumentField.OpenDate,  # 上市日
                    'ExpireDate': InstrumentField.ExpireDate,  # 到期日
                    'StartDelivDate': InstrumentField.StartDelivDate,  # 开始交割日
                    'EndDelivDate': InstrumentField.EndDelivDate,  # 结束交割日
                    'InstLifePhase': InstrumentField.InstLifePhase,  # 合约生命周期状态
                    'IsTrading': InstrumentField.IsTrading,  # 当前是否交易
                    'PositionType': InstrumentField.PositionType,  # 持仓类型
                    'PositionDateType': InstrumentField.PositionDateType,  # 持仓日期类型
                    'LongMarginRatio': InstrumentField.LongMarginRatio,  # 多头保证金率
                    'ShortMarginRatio': InstrumentField.ShortMarginRatio,  # 空头保证金率
                    'MaxMarginSideAlgorithm': InstrumentField.MaxMarginSideAlgorithm,  # 是否使用大额单边保证金算法
                    'UnderlyingInstrID': InstrumentField.UnderlyingInstrID,  # 基础商品代码
                    'StrikePrice': InstrumentField.StrikePrice,  # 执行价
                    'OptionsType': InstrumentField.OptionsType,  # 期权类型
                    'UnderlyingMultiple': InstrumentField.UnderlyingMultiple,  # 合约基础商品乘数
                    'CombinationType': InstrumentField.CombinationType  # 组合类型
                }
                self.__rsp_QryInstrument['results'].append(Instrument)
            if IsLast:
                self.__rsp_QryInstrument['event'].set()

    def OnRspQryInstrumentMarginRate(self, InstrumentMarginRateField, RspInfo, RequestID, IsLast):
        """ 请求查询合约保证金率响应 """
        if RequestID == self.__rsp_QryInstrumentMarginRate['RequestID']:
            if RspInfo is not None:
                self.__rsp_QryInstrumentMarginRate.update(RspInfo)
            if InstrumentMarginRateField is not None:
                InstrumentMarginRate = {
                    'InstrumentID': InstrumentMarginRateField.InstrumentID,  # 合约代码
                    'InvestorRange': InstrumentMarginRateField.InvestorRange,  # 投资者范围
                    'BrokerID': InstrumentMarginRateField.BrokerID,  # 经纪公司代码
                    'InvestorID': InstrumentMarginRateField.InvestorID,  # 投资者代码
                    'HedgeFlag': InstrumentMarginRateField.HedgeFlag,  # 投机套保标志
                    'LongMarginRatioByMoney': InstrumentMarginRateField.LongMarginRatioByMoney,  # 多头保证金率
                    'LongMarginRatioByVolume': InstrumentMarginRateField.LongMarginRatioByVolume,  # 多头保证金费
                    'ShortMarginRatioByMoney': InstrumentMarginRateField.ShortMarginRatioByMoney,  # 空头保证金率
                    'ShortMarginRatioByVolume': InstrumentMarginRateField.ShortMarginRatioByVolume,  # 空头保证金费
                    'IsRelative': InstrumentMarginRateField.IsRelative,  # 是否相对交易所收取
                }
                self.__rsp_QryInstrumentMarginRate['results'].append(InstrumentMarginRate)
            if IsLast:
                self.__rsp_QryInstrumentMarginRate['event'].set()

    def OnRspQryInstrumentCommissionRate(self, nstrumentCommissionRateField, RspInfoField, RequestID, IsLast):
        """ 请求查询合约手续费率响应 """
        if RequestID == self.__rsp_QryInstrumentCommissionRate['RequestID']:
            if RspInfoField is not None:
                RspInfo = {
                    'ErrorID': RspInfoField.ErrorID,  # 错误代码
                    'ErrorMsg': RspInfoField.ErrorMsg  # 错误信息
                }
                self.__rsp_QryInstrumentCommissionRate.update(RspInfo)
            if nstrumentCommissionRateField is not None:
                InstrumentCommissionRate = {
                    'InstrumentID': nstrumentCommissionRateField.InstrumentID,  # 合约代码
                    'InvestorRange': nstrumentCommissionRateField.InvestorRange,  # 投资者范围
                    'BrokerID': nstrumentCommissionRateField.BrokerID,  # 经纪公司代码
                    'InvestorID': nstrumentCommissionRateField.InvestorID,  # 投资者代码
                    'OpenRatioByMoney': nstrumentCommissionRateField.OpenRatioByMoney,  # 开仓手续费率
                    'OpenRatioByVolume': nstrumentCommissionRateField.OpenRatioByVolume,  # 开仓手续费
                    'CloseRatioByMoney': nstrumentCommissionRateField.CloseRatioByMoney,  # 平仓手续费率
                    'CloseRatioByVolume': nstrumentCommissionRateField.CloseRatioByVolume,  # 平仓手续费
                    'CloseTodayRatioByMoney': nstrumentCommissionRateField.CloseTodayRatioByMoney,  # 平今手续费率
                    'CloseTodayRatioByVolume': nstrumentCommissionRateField.CloseTodayRatioByVolume  # 平今手续费
                }
                self.__rsp_QryInstrumentCommissionRate['results'].append(InstrumentCommissionRate)
            if IsLast:
                self.__rsp_QryInstrumentCommissionRate['event'].set()

    def OnRspQryOrder(self, OrderField, RspInfo, RequestID, IsLast):
        """请求查询报单响应"""
        # self.__user.OnRspQryOrder(Order, RspInfo, RequestID, IsLast)  # 转到user回调函数
        if RequestID == self.__rsp_QryOrder['RequestID']:
            if RspInfo is not None:
                self.__rsp_QryOrder.update(RspInfo)
            if OrderField is not None:
                Order = {
                    'BrokerID': OrderField.BrokerID,  # 经纪公司代码
                    'InvestorID': OrderField.InvestorID,  # 投资者代码
                    'InstrumentID': OrderField.InstrumentID,  # 合约代码
                    'OrderRef': OrderField.OrderRef,  # 报单引用
                    'UserID': OrderField.UserID,  # 用户代码
                    'OrderPriceType': OrderField.OrderPriceType,  # 报单价格条件
                    'Direction': OrderField.Direction,  # 买卖方向
                    'CombOffsetFlag': OrderField.CombOffsetFlag,  # 组合开平标志
                    'CombHedgeFlag': OrderField.CombHedgeFlag,  # 组合投机套保标志
                    'LimitPrice': OrderField.LimitPrice,  # 价格
                    'VolumeTotalOriginal': OrderField.VolumeTotalOriginal,  # 数量
                    'TimeCondition': OrderField.TimeCondition,  # 有效期类型
                    'GTDDate': OrderField.GTDDate,  # GTD日期
                    'VolumeCondition': OrderField.VolumeCondition,  # 成交量类型
                    'MinVolume': OrderField.MinVolume,  # 最小成交量
                    'ContingentCondition': OrderField.ContingentCondition,  # 触发条件
                    'StopPrice': OrderField.StopPrice,  # 止损价
                    'ForceCloseReason': OrderField.ForceCloseReason,  # 强平原因
                    'IsAutoSuspend': OrderField.IsAutoSuspend,  # 自动挂起标志
                    'BusinessUnit': OrderField.BusinessUnit,  # 业务单元
                    'RequestID': OrderField.RequestID,  # 请求编号
                    'OrderLocalID': OrderField.OrderLocalID,  # 本地报单编号
                    'ExchangeID': OrderField.ExchangeID,  # 交易所代码
                    'ParticipantID': OrderField.ParticipantID,  # 会员代码
                    'ClientID': OrderField.ClientID,  # 客户代码
                    'ExchangeInstID': OrderField.ExchangeInstID,  # 合约在交易所的代码
                    'TraderID': OrderField.TraderID,  # 交易所交易员代码
                    'InstallID': OrderField.InstallID,  # 安装编号
                    'OrderSubmitStatus': OrderField.OrderSubmitStatus,  # 报单提交状态
                    'NotifySequence': OrderField.NotifySequence,  # 报单提示序号
                    'TradingDay': OrderField.TradingDay,  # 交易日
                    'SettlementID': OrderField.SettlementID,  # 结算编号
                    'OrderSysID': OrderField.OrderSysID,  # 报单编号
                    'OrderSource': OrderField.OrderSource,  # 报单来源
                    'OrderStatus': OrderField.OrderStatus,  # 报单状态
                    'OrderType': OrderField.OrderType,  # 报单类型
                    'VolumeTraded': OrderField.VolumeTraded,  # 今成交数量
                    'VolumeTotal': OrderField.VolumeTotal,  # 剩余数量
                    'InsertDate': OrderField.InsertDate,  # 报单日期
                    'InsertTime': OrderField.InsertTime,  # 委托时间
                    'ActiveTime': OrderField.ActiveTime,  # 激活时间
                    'SuspendTime': OrderField.SuspendTime,  # 挂起时间
                    'UpdateTime': OrderField.UpdateTime,  # 最后修改时间
                    'CancelTime': OrderField.CancelTime,  # 撤销时间
                    'ActiveTraderID': OrderField.ActiveTraderID,  # 最后修改交易所交易员代码
                    'ClearingPartID': OrderField.ClearingPartID,  # 结算会员编号
                    'SequenceNo': OrderField.SequenceNo,  # 序号
                    'FrontID': OrderField.FrontID,  # 前置编号
                    'SessionID': OrderField.SessionID,  # 会话编号
                    'UserProductInfo': OrderField.UserProductInfo,  # 用户端产品信息
                    'StatusMsg': OrderField.StatusMsg,  # 状态信息
                    'UserForceClose': OrderField.UserForceClose,  # 用户强评标志
                    'ActiveUserID': OrderField.ActiveUserID,  # 操作用户代码
                    'BrokerOrderSeq': OrderField.BrokerOrderSeq,  # 经纪公司报单编号
                    'RelativeOrderSysID': OrderField.RelativeOrderSysID,  # 相关报单
                    'ZCETotalTradedVolume': OrderField.ZCETotalTradedVolume,  # 郑商所成交数量
                    'IsSwapOrder': OrderField.IsSwapOrder,  # 互换单标志
                    'BranchID': OrderField.BranchID,  # 营业部编号
                    'InvestUnitID': OrderField.InvestUnitID,  # 投资单元代码
                    'AccountID': OrderField.AccountID,  # 资金账号
                    'CurrencyID': OrderField.CurrencyID,  # 币种代码
                    'IPAddress': OrderField.IPAddress,  # IP地址
                    'MacAddress': OrderField.MacAddress  # Mac地址
                }
                self.__rsp_QryOrder['results'].append(Order)
            if IsLast:
                self.__rsp_QryOrder['event'].set()
        Order = Utils.code_transform(Order)
        series_order = Series(Order)
        self.__df_qry_order = DataFrame.append(self.__df_qry_order, other=series_order, ignore_index=True)

    def OnRspQryTrade(self, TradeField, RspInfo, RequestID, IsLast):
        """请求查询成交单响应"""
        # self.__user.OnRspQryTrade(Trade, RspInfo, RequestID, IsLast)  # 转到user回调函数
        if RequestID == self.__rsp_QryTrade['RequestID']:
            if RspInfo is not None:
                self.__rsp_QryTrade.update(RspInfo)
            if TradeField is not None:
                Trade = {
                    'BrokerID': TradeField.BrokerID,  # 经纪公司代码
                    'InvestorID': TradeField.InvestorID,  # 投资者代码
                    'InstrumentID': TradeField.InstrumentID,  # 合约代码
                    'OrderRef': TradeField.OrderRef,  # 报单引用
                    'UserID': TradeField.UserID,  # 用户代码
                    'ExchangeID': TradeField.ExchangeID,  # 交易所代码
                    'TradeID': TradeField.TradeID,  # 成交编号
                    'Direction': TradeField.Direction,  # 买卖方向
                    'OrderSysID': TradeField.OrderSysID,  # 报单编号
                    'ParticipantID': TradeField.ParticipantID,  # 会员代码
                    'ClientID': TradeField.ClientID,  # 客户代码
                    'TradingRole': TradeField.TradingRole,  # 交易角色
                    'ExchangeInstID': TradeField.ExchangeInstID,  # 合约在交易所的代码
                    'OffsetFlag': TradeField.OffsetFlag,  # 开平标志
                    'HedgeFlag': TradeField.HedgeFlag,  # 投机套保标志
                    'Price': TradeField.Price,  # 价格
                    'Volume': TradeField.Volume,  # 数量
                    'TradeDate': TradeField.TradeDate,  # 成交时期
                    'TradeTime': TradeField.TradeTime,  # 成交时间
                    'TradeType': TradeField.TradeType,  # 成交类型
                    'PriceSource': TradeField.PriceSource,  # 成交价来源
                    'TraderID': TradeField.TraderID,  # 交易所交易员代码
                    'OrderLocalID': TradeField.OrderLocalID,  # 本地报单编号
                    'ClearingPartID': TradeField.ClearingPartID,  # 结算会员编号
                    'BusinessUnit': TradeField.BusinessUnit,  # 业务单元
                    'SequenceNo': TradeField.SequenceNo,  # 序号
                    'TradingDay': TradeField.TradingDay,  # 交易日
                    'SettlementID': TradeField.SettlementID,  # 结算编号
                    'BrokerOrderSeq': TradeField.BrokerOrderSeq,  # 经纪公司报单编号
                    'TradeSource': TradeField.TradeSource  # 成交来源
                }
                self.__rsp_QryTrade['results'].append(Trade)
            if IsLast:
                self.__rsp_QryTrade['event'].set()
        Trade = Utils.code_transform(Trade)
        series_trade = Series(Trade)
        self.__df_qry_trade = DataFrame.append(self.__df_qry_trade, other=series_trade, ignore_index=True)

    def OnRspQryInvestorPosition(self, InvestorPositionField, RspInfo, RequestID, IsLast):
        """ 请求查询投资者持仓响应 """
        print(">>>PyCTP_Trade.OnRspQryInvestorPosition() called")
        # self.__user.write_log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'OnRspQryInvestorPosition', '查询投资者持仓响应', str(InvestorPosition))
        if RequestID == self.__rsp_QryInvestorPosition['RequestID']:
            if RspInfo is not None:
                self.__rsp_QryInvestorPosition.update(RspInfo)
            if InvestorPositionField is not None:
                InvestorPosition = {
                    'InstrumentID': InvestorPositionField.InstrumentID,  # 合约代码
                    'BrokerID': InvestorPositionField.BrokerID,  # 经纪公司代码
                    'InvestorID': InvestorPositionField.InvestorID,  # 投资者代码
                    'PosiDirection': InvestorPositionField.PosiDirection,  # 持仓多空方向
                    'HedgeFlag': InvestorPositionField.HedgeFlag,  # 投机套保标志
                    'PositionDate': InvestorPositionField.PositionDate,  # 持仓日期
                    'YdPosition': InvestorPositionField.YdPosition,  # 上日持仓
                    'Position': InvestorPositionField.Position,  # 今日持仓
                    'LongFrozen': InvestorPositionField.LongFrozen,  # 多头冻结
                    'ShortFrozen': InvestorPositionField.ShortFrozen,  # 空头冻结
                    'LongFrozenAmount': InvestorPositionField.LongFrozenAmount,  # 开仓冻结金额
                    'ShortFrozenAmount': InvestorPositionField.ShortFrozenAmount,  # 开仓冻结金额
                    'OpenVolume': InvestorPositionField.OpenVolume,  # 开仓量
                    'CloseVolume': InvestorPositionField.CloseVolume,  # 平仓量
                    'OpenAmount': InvestorPositionField.OpenAmount,  # 开仓金额
                    'CloseAmount': InvestorPositionField.CloseAmount,  # 平仓金额
                    'PositionCost': InvestorPositionField.PositionCost,  # 持仓成本
                    'PreMargin': InvestorPositionField.PreMargin,  # 上次占用的保证金
                    'UseMargin': InvestorPositionField.UseMargin,  # 占用的保证金
                    'FrozenMargin': InvestorPositionField.FrozenMargin,  # 冻结的保证金
                    'FrozenCash': InvestorPositionField.FrozenCash,  # 冻结的资金
                    'FrozenCommission': InvestorPositionField.FrozenCommission,  # 冻结的手续费
                    'CashIn': InvestorPositionField.CashIn,  # 资金差额
                    'Commission': InvestorPositionField.Commission,  # 手续费
                    'CloseProfit': InvestorPositionField.CloseProfit,  # 平仓盈亏
                    'PositionProfit': InvestorPositionField.PositionProfit,  # 持仓盈亏
                    'PreSettlementPrice': InvestorPositionField.PreSettlementPrice,  # 上次结算价
                    'SettlementPrice': InvestorPositionField.SettlementPrice,  # 本次结算价
                    'TradingDay': InvestorPositionField.TradingDay,  # 交易日
                    'SettlementID': InvestorPositionField.SettlementID,  # 结算编号
                    'OpenCost': InvestorPositionField.OpenCost,  # 开仓成本
                    'ExchangeMargin': InvestorPositionField.ExchangeMargin,  # 交易所保证金
                    'CombPosition': InvestorPositionField.CombPosition,  # 组合成交形成的持仓
                    'CombLongFrozen': InvestorPositionField.CombLongFrozen,  # 组合多头冻结
                    'CombShortFrozen': InvestorPositionField.CombShortFrozen,  # 组合空头冻结
                    'CloseProfitByDate': InvestorPositionField.CloseProfitByDate,  # 逐日盯市平仓盈亏
                    'CloseProfitByTrade': InvestorPositionField.CloseProfitByTrade,  # 逐笔对冲平仓盈亏
                    'TodayPosition': InvestorPositionField.TodayPosition,  # 今日持仓
                    'MarginRateByMoney': InvestorPositionField.MarginRateByMoney,  # 保证金率
                    'MarginRateByVolume': InvestorPositionField.MarginRateByVolume,  # 保证金率(按手数)
                    'StrikeFrozen': InvestorPositionField.StrikeFrozen,  # 执行冻结
                    'StrikeFrozenAmount': InvestorPositionField.StrikeFrozenAmount,  # 执行冻结金额
                    'AbandonFrozen': InvestorPositionField.AbandonFrozen   # 放弃执行冻结
                }
                self.__rsp_QryInvestorPosition['results'].append(InvestorPosition)
            if IsLast:
                self.__rsp_QryInvestorPosition['event'].set()

    def OnRspQryInvestorPositionDetail(self, InvestorPositionDetailField, RspInfoField, RequestID, IsLast):
        """ 请求查询投资者持仓明细响应 """
        # self.__user.write_log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'OnRspQryInvestorPositionDetail', '查询投资者持仓明细响应', str(InvestorPositionDetail))
        if RequestID == self.__rsp_QryInvestorPositionDetail['RequestID']:
            if RspInfoField is not None:
                RspInfo = {
                    'ErrorID': RspInfoField.ErrorID,  # 错误代码
                    'ErrorMsg': RspInfoField.ErrorMsg  # 错误信息
                }
                self.__rsp_QryInvestorPositionDetail.update(RspInfo)
            if InvestorPositionDetailField is not None:
                InvestorPositionDetail = {
                    'InstrumentID': InvestorPositionDetailField.InstrumentID,  # 合约代码
                    'BrokerID': InvestorPositionDetailField.BrokerID,  # 经纪公司代码
                    'InvestorID': InvestorPositionDetailField.InvestorID,  # 投资者代码
                    'HedgeFlag': InvestorPositionDetailField.HedgeFlag,  # 投机套保标志
                    'Direction': InvestorPositionDetailField.Direction,  # 买卖
                    'OpenDate': InvestorPositionDetailField.OpenDate,  # 开仓日期
                    'TradeID': InvestorPositionDetailField.TradeID,  # 成交编号
                    'Volume': InvestorPositionDetailField.Volume,  # 数量
                    'OpenPrice': InvestorPositionDetailField.OpenPrice,  # 开仓价
                    'TradingDay': InvestorPositionDetailField.TradingDay,  # 交易日
                    'SettlementID': InvestorPositionDetailField.SettlementID,  # 结算编号
                    'TradeType': InvestorPositionDetailField.TradeType,  # 成交类型
                    'CombInstrumentID': InvestorPositionDetailField.CombInstrumentID,  # 组合合约代码
                    'ExchangeID': InvestorPositionDetailField.ExchangeID,  # 交易所代码
                    'CloseProfitByDate': InvestorPositionDetailField.CloseProfitByDate,  # 逐日盯市平仓盈亏
                    'CloseProfitByTrade': InvestorPositionDetailField.CloseProfitByTrade,  # 逐笔对冲平仓盈亏
                    'PositionProfitByDate': InvestorPositionDetailField.PositionProfitByDate,  # 逐日盯市持仓盈亏
                    'PositionProfitByTrade': InvestorPositionDetailField.PositionProfitByTrade,  # 逐笔对冲持仓盈亏
                    'Margin': InvestorPositionDetailField.Margin,  # 投资者保证金
                    'ExchMargin': InvestorPositionDetailField.ExchMargin,  # 交易所保证金
                    'MarginRateByMoney': InvestorPositionDetailField.MarginRateByMoney,  # 保证金率
                    'MarginRateByVolume': InvestorPositionDetailField.MarginRateByVolume,  # 保证金率(按手数)
                    'LastSettlementPrice': InvestorPositionDetailField.LastSettlementPrice,  # 昨结算价
                    'SettlementPrice': InvestorPositionDetailField.SettlementPrice,  # 结算价
                    'CloseVolume': InvestorPositionDetailField.CloseVolume,  # 平仓量
                    'CloseAmount': InvestorPositionDetailField.CloseAmount, # 平仓金额
                }
                self.__rsp_QryInvestorPositionDetail['results'].append(InvestorPositionDetail)
            if IsLast:
                self.__rsp_QryInvestorPositionDetail['event'].set()

    def OnRspQryTradingAccount(self, TradingAccountField, RspInfoField, RequestID, IsLast):
        """ 请求查询资金账户响应 """
        # self.__user.write_log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'OnRspQryTradingAccount', '查询资金账户响应', str(TradingAccount))
        if RequestID == self.__rsp_QryTradingAccount['RequestID']:
            if RspInfoField is not None:
                RspInfo = {
                    'ErrorID': RspInfoField.ErrorID,  # 错误代码
                    'ErrorMsg': RspInfoField.ErrorMsg  # 错误信息
                }
                self.__rsp_QryTradingAccount.update(RspInfo)
            if TradingAccountField is not None:
                TradingAccount = {
                    'BrokerID': TradingAccountField.BrokerID,  # 经纪公司代码
                    'AccountID': TradingAccountField.AccountID,  # 投资者帐号
                    'PreMortgage': TradingAccountField.PreMortgage,  # 上次质押金额
                    'PreCredit': TradingAccountField.PreCredit,  # 上次信用额度
                    'PreDeposit': TradingAccountField.PreDeposit,  # 上次存款额
                    'PreBalance': TradingAccountField.PreBalance,  # 上次结算准备金
                    'PreMargin': TradingAccountField.PreMargin,  # 上次占用的保证金
                    'InterestBase': TradingAccountField.InterestBase,  # 利息基数
                    'Interest': TradingAccountField.Interest,  # 利息收入
                    'Deposit': TradingAccountField.Deposit,  # 入金金额
                    'Withdraw': TradingAccountField.Withdraw,  # 出金金额
                    'FrozenMargin': TradingAccountField.FrozenMargin,  # 冻结的保证金
                    'FrozenCash': TradingAccountField.FrozenCash,  # 冻结的资金
                    'FrozenCommission': TradingAccountField.FrozenCommission,  # 冻结的手续费
                    'CurrMargin': TradingAccountField.CurrMargin,  # 当前保证金总额
                    'CashIn': TradingAccountField.CashIn,  # 资金差额
                    'Commission': TradingAccountField.Commission,  # 手续费
                    'CloseProfit': TradingAccountField.CloseProfit,  # 平仓盈亏
                    'PositionProfit': TradingAccountField.PositionProfit,  # 持仓盈亏
                    'Balance': TradingAccountField.Balance,  # 期货结算准备金
                    'Available': TradingAccountField.Available,  # 可用资金
                    'WithdrawQuota': TradingAccountField.WithdrawQuota,  # 可取资金
                    'Reserve': TradingAccountField.Reserve,  # 基本准备金
                    'TradingDay': TradingAccountField.TradingDay,  # 交易日
                    'SettlementID': TradingAccountField.SettlementID,  # 结算编号
                    'Credit': TradingAccountField.Credit,  # 信用额度
                    'Mortgage': TradingAccountField.Mortgage,  # 质押金额
                    'ExchangeMargin': TradingAccountField.ExchangeMargin,  # 交易所保证金
                    'DeliveryMargin': TradingAccountField.DeliveryMargin,  # 投资者交割保证金
                    'ExchangeDeliveryMargin': TradingAccountField.ExchangeDeliveryMargin,  # 交易所交割保证金
                    'ReserveBalance': TradingAccountField.ReserveBalance,  # 保底期货结算准备金
                    'CurrencyID': TradingAccountField.CurrencyID,  # 币种代码
                    'PreFundMortgageIn': TradingAccountField.PreFundMortgageIn,  # 上次货币质入金额
                    'PreFundMortgageOut': TradingAccountField.PreFundMortgageOut,  # 上次货币质出金额
                    'FundMortgageIn': TradingAccountField.FundMortgageIn,  # 货币质入金额
                    'FundMortgageOut': TradingAccountField.FundMortgageOut,  # 货币质出金额
                    'FundMortgageAvailable': TradingAccountField.FundMortgageAvailable,  # 货币质押余额
                    'MortgageableFund': TradingAccountField.MortgageableFund,  # 可质押货币金额
                    'SpecProductMargin': TradingAccountField.SpecProductMargin,  # 特殊产品占用保证金
                    'SpecProductFrozenMargin': TradingAccountField.SpecProductFrozenMargin,  # 特殊产品冻结保证金
                    'SpecProductCommission': TradingAccountField.SpecProductCommission,  # 特殊产品手续费
                    'SpecProductFrozenCommission': TradingAccountField.SpecProductFrozenCommission,  # 特殊产品冻结手续费
                    'SpecProductPositionProfit': TradingAccountField.SpecProductPositionProfit,  # 特殊产品持仓盈亏
                    'SpecProductCloseProfit': TradingAccountField.SpecProductCloseProfit,  # 特殊产品平仓盈亏
                    'SpecProductPositionProfitByAlg': TradingAccountField.SpecProductPositionProfitByAlg,  # 根据持仓盈亏算法计算的特殊产品持仓盈亏
                    'SpecProductExchangeMargin': TradingAccountField.SpecProductExchangeMargin  # 特殊产品交易所保证金
                }
                self.__rsp_QryTradingAccount['results'].append(TradingAccount)
            if IsLast:
                self.__rsp_QryTradingAccount['event'].set()

    def OnRspQryInvestor(self, InvestorField, RspInfo, RequestID, IsLast):
        """ 请求查询投资者响应 """
        if RequestID == self.__rsp_QryInvestor['RequestID']:
            if RspInfo is not None:
                self.__rsp_QryInvestor.update(RspInfo)
            if InvestorField is not None:
                Investor = {
                    'InvestorID': InvestorField.InvestorID,  # 投资者代码
                    'BrokerID': InvestorField.BrokerID,  # 经纪公司代码
                    'InvestorGroupID': InvestorField.InvestorGroupID,  # 投资者分组代码
                    'InvestorName': InvestorField.InvestorName,  # 投资者名称
                    'IdentifiedCardType': InvestorField.IdentifiedCardType,  # 证件类型
                    'IdentifiedCardNo': InvestorField.IdentifiedCardNo,  # 证件号码
                    'IsActive': InvestorField.IsActive,  # 是否活跃
                    'Telephone': InvestorField.Telephone,  # 联系电话
                    'Address': InvestorField.Address,  # 通讯地址
                    'OpenDate': InvestorField.OpenDate,  # 开户日期
                    'Mobile': InvestorField.Mobile,  # 手机
                    'CommModelID': InvestorField.CommModelID,  # 手续费率模板代码
                    'MarginModelID': InvestorField.MarginModelID  # 保证金率模板代码
                }
                self.__rsp_QryInvestor['results'].append(Investor)
            if IsLast:
                self.__rsp_QryInvestor['event'].set()

    def OnRspQryExchange(self, ExchangeField, RspInfo, RequestID, IsLast):
        """ 请求查询交易所响应 """
        if RequestID == self.__rsp_QryExchange['RequestID']:
            if RspInfo is not None:
                self.__rsp_QryExchange.update(RspInfo)
            if ExchangeField is not None:
                Exchange ={
                    'ExchangeID': ExchangeField.ExchangeID,  # 交易所代码
                    'ExchangeName': ExchangeField.ExchangeName,  # 交易所名称
                    'ExchangeProperty': ExchangeField.ExchangeProperty  # 交易所属性
                }
                self.__rsp_QryExchange['results'].append(Exchange)
            if IsLast:
                self.__rsp_QryExchange['event'].set()

    def OnRspQryDepthMarketData(self, DepthMarketDataField, RspInfo, RequestID, IsLast):
        """ 请求查询行情响应 """
        if RequestID == self.__rsp_QryDepthMarketData['RequestID']:
            if RspInfo is not None:
                self.__rsp_QryDepthMarketData.update(RspInfo)
            if DepthMarketDataField is not None:
                DepthMarketData = {
                    'TradingDay': DepthMarketDataField.TradingDay,  # 交易日
                    'InstrumentID': DepthMarketDataField.InstrumentID,  # 合约代码
                    'ExchangeID': DepthMarketDataField.ExchangeID,  # 交易所代码
                    'ExchangeInstID': DepthMarketDataField.ExchangeInstID,  # 合约在交易所的代码
                    'LastPrice': DepthMarketDataField.LastPrice,  # 最新价
                    'PreSettlementPrice': DepthMarketDataField.PreSettlementPrice,  # 上次结算价
                    'PreClosePrice': DepthMarketDataField.PreClosePrice,  # 昨收盘
                    'PreOpenInterest': DepthMarketDataField.PreOpenInterest,  # 昨持仓量
                    'OpenPrice': DepthMarketDataField.OpenPrice,  # 今开盘
                    'HighestPrice': DepthMarketDataField.HighestPrice,  # 最高价
                    'LowestPrice': DepthMarketDataField.LowestPrice,  # 最低价
                    'Volume': DepthMarketDataField.Volume,  # 数量
                    'Turnover': DepthMarketDataField.Turnover,  # 成交金额
                    'OpenInterest': DepthMarketDataField.OpenInterest,  # 持仓量
                    'ClosePrice': DepthMarketDataField.ClosePrice,  # 今收盘
                    'SettlementPrice': DepthMarketDataField.SettlementPrice,  # 本次结算价
                    'UpperLimitPrice': DepthMarketDataField.UpperLimitPrice,  # 涨停板价
                    'LowerLimitPrice': DepthMarketDataField.LowerLimitPrice,  # 跌停板价
                    'PreDelta': DepthMarketDataField.PreDelta,  # 昨虚实度
                    'CurrDelta': DepthMarketDataField.CurrDelta,  # 今虚实度
                    'UpdateTime': DepthMarketDataField.UpdateTime,  # 最后修改时间
                    'UpdateMillisec': DepthMarketDataField.UpdateMillisec,  # 最后修改毫秒
                    'BidPrice1': DepthMarketDataField.BidPrice1,  # 申买价一
                    'BidVolume1': DepthMarketDataField.BidVolume1,  # 申买量一
                    'AskPrice1': DepthMarketDataField.AskPrice1,  # 申卖价一
                    'AskVolume1': DepthMarketDataField.AskVolume1,  # 申卖量一
                    'AveragePrice': DepthMarketDataField.AveragePrice,  # 当日均价
                    'ActionDay': DepthMarketDataField.ActionDay  # 业务日期
                }
                self.__rsp_QryDepthMarketData['results'].append(DepthMarketData)
            if IsLast:
                self.__rsp_QryDepthMarketData['event'].set()

    def OnRspQryTradingCode(self, TradingCodeField, RspInfo, RequestID, IsLast):
        """查询交易编码响应"""
        if RequestID == self.__rsp_QryTradingCode['RequestID']:
            if RspInfo is not None:
                self.__rsp_QryTradingCode.update(RspInfo)
            if TradingCodeField is not None:
                TradingCode = {
                'InvestorID': TradingCodeField.InvestorID,  # 投资者代码
                'BrokerID': TradingCodeField.BrokerID,  # 经纪公司代码
                'ExchangeID': TradingCodeField.ExchangeID,  # 交易所代码
                'ClientID': TradingCodeField.ClientID,  # 客户代码
                'IsActive': TradingCodeField.IsActive,  # 是否活跃
                'ClientIDType': TradingCodeField.ClientIDType  # 交易编码类型
                }
                self.__rsp_QryTradingCode['results'].append(TradingCode)
            if IsLast:
                self.__rsp_QryTradingCode['event'].set()

    def OnRspOrderInsert(self, InputOrderField, RspInfo, RequestID, IsLast):
        """ 报单录入请求响应 """
        # 报单错误时响应
        # print('PyCTP_Trade.OnRspOrderInsert()', 'OrderRef:', InputOrderField['OrderRef'], 'InputOrderField:', InputOrderField, 'RspInfo:', RspInfo, 'RequestID:', RequestID, 'IsLast:', IsLast)
        # InputOrderField = Utils.code_transform(InputOrderField)
        # RspInfo = Utils.code_transform(RspInfo)
        # RequestID = Utils.code_transform(RequestID)
        # IsLast = Utils.code_transform(IsLast)

        # if self.__rsp_OrderInsert['RequestID'] == RequestID \
        #         and self.__rsp_OrderInsert['InputOrderField']['OrderRef'].decode() == InputOrderField['OrderRef']:
        if self.__rsp_OrderInsert['RequestID'] == RequestID \
                and self.__rsp_OrderInsert['InputOrder'].OrderRef == InputOrderField.OrderRef:
            if RspInfo is not None and RspInfo['ErrorID'] != 0:
                for i in self.__user.get_list_strategy():  # 转到strategy回调函数
                    if InputOrderField.OrderRef[-2:] == i.get_strategy_id():
                        InputOrder = {
                            'BrokerID': InputOrderField.BrokerID,  # 经纪公司代码
                            'InvestorID': InputOrderField.InvestorID,  # 投资者代码
                            'InstrumentID': InputOrderField.InstrumentID,  # 合约代码
                            'OrderRef': InputOrderField.OrderRef,  # 报单引用
                            'UserID': InputOrderField.UserID,  # 用户代码
                            'OrderPriceType': InputOrderField.OrderPriceType,  # 报单价格条件
                            'Direction': InputOrderField.Direction,  # 买卖方向
                            'CombOffsetFlag': InputOrderField.CombOffsetFlag,  # 组合开平标志
                            'CombHedgeFlag': InputOrderField.CombHedgeFlag,  # 组合投机套保标志
                            'LimitPrice': InputOrderField.LimitPrice,  # 价格
                            'VolumeTotalOriginal': InputOrderField.VolumeTotalOriginal,  # 数量
                            'TimeCondition': InputOrderField.TimeCondition,  # 有效期类型
                            'GTDDate': InputOrderField.GTDDate,  # GTD日期
                            'VolumeCondition': InputOrderField.VolumeCondition,  # 成交量类型
                            'MinVolume': InputOrderField.MinVolume,  # 最小成交量
                            'ContingentCondition': InputOrderField.ContingentCondition,  # 触发条件
                            'StopPrice': InputOrderField.StopPrice,  # 止损价
                            'ForceCloseReason': InputOrderField.ForceCloseReason,  # 强平原因
                            'IsAutoSuspend': InputOrderField.IsAutoSuspend,  # 自动挂起标志
                            'BusinessUnit': InputOrderField.BusinessUnit,  # 业务单元
                            'RequestID': InputOrderField.RequestID,  # 请求编号
                            'UserForceClose': InputOrderField.UserForceClose,  # 用户强评标志
                            'IsSwapOrder': InputOrderField.IsSwapOrder,  # 互换单标志
                            'ExchangeID': InputOrderField.ExchangeID,  # 交易所代码
                            'InvestUnitID': InputOrderField.InvestUnitID,  # 投资单元代码
                            'AccountID': InputOrderField.AccountID,  # 资金账号
                            'CurrencyID': InputOrderField.CurrencyID,  # 币种代码
                            'ClientID': InputOrderField.ClientID,  # 交易编码
                            'IPAddress': InputOrderField.IPAddress,  # IP地址
                            'MacAddress': InputOrderField.MacAddress  # Mac地址
                        }
                        i.OnRspOrderInsert(InputOrder, RspInfo, RequestID, IsLast)
                self.__rsp_OrderInsert.update(RspInfo)
                self.__rsp_OrderInsert['event'].set()

    def OnRspOrderAction(self, InputOrderActionField, RspInfo, RequestID, IsLast):
        """报单操作请求响应:撤单操作响应"""
        # InputOrderAction = Utils.code_transform(InputOrderAction)
        # RspInfo = Utils.code_transform(RspInfo)
        # RequestID = Utils.code_transform(RequestID)
        # IsLast = Utils.code_transform(IsLast)
        # print('PyCTP_Trade.OnRspOrderAction()', 'OrderRef:', InputOrderAction['OrderRef'], 'InputOrderAction:', InputOrderAction, 'RspInfo:', RspInfo, 'RequestID:', RequestID, 'IsLast:', IsLast)
        # if hasattr(self, '_PyCTP_Trader_API__rsp_OrderInsert'):
        # if self.__rsp_OrderAction['InputOrder']['OrderRef'] == InputOrderAction['OrderRef']:
        #     self.__rsp_OrderInsert['event'].set()
        InputOrderAction = {
            'BrokerID': InputOrderActionField.BrokerID,  # 经纪公司代码
            'InvestorID': InputOrderActionField.InvestorID,  # 投资者代码
            'OrderActionRef': InputOrderActionField.OrderActionRef,  # 报单操作引用
            'OrderRef': InputOrderActionField.OrderRef,  # 报单引用
            'RequestID': InputOrderActionField.RequestID,  # 请求编号
            'FrontID': InputOrderActionField.FrontID,  # 前置编号
            'SessionID': InputOrderActionField.SessionID,  # 会话编号
            'ExchangeID': InputOrderActionField.ExchangeID,  # 交易所代码
            'OrderSysID': InputOrderActionField.OrderSysID,  # 报单编号
            'ActionFlag': InputOrderActionField.ActionFlag,  # 操作标志
            'LimitPrice': InputOrderActionField.LimitPrice,  # 价格
            'VolumeChange': InputOrderActionField.VolumeChange,  # 数量变化
            'UserID': InputOrderActionField.UserID,  # 用户代码
            'InstrumentID': InputOrderActionField.InstrumentID,  # 合约代码
            'InvestUnitID': InputOrderActionField.InvestUnitID,  # 投资单元代码
            'IPAddress': InputOrderActionField.IPAddress,  # IP地址
            'MacAddress': InputOrderActionField.MacAddress  # Mac地址
        }
        for i in self.__user.get_list_strategy():  # 转到strategy回调函数
            if InputOrderAction['OrderRef'][-2:] == i.get_strategy_id():
                i.OnRspOrderAction(InputOrderAction, RspInfo, RequestID, IsLast)

    def OnRtnOrder(self, OrderField):
        """报单通知"""
        # Order = copy.deepcopy(Order_input)
        # print(">>>PyCTP_Trade.OnRtnOrder() Order =", Order)
        # Order = Utils.code_transform(Order)
        # t = datetime.now().strftime("%Y-%m-%d %H:%M:%S %f")
        # Order['time'] = t
        Order = {
            'BrokerID': OrderField.BrokerID,  # 经纪公司代码
            'InvestorID': OrderField.InvestorID,  # 投资者代码
            'InstrumentID': OrderField.InstrumentID,  # 合约代码
            'OrderRef': OrderField.OrderRef,  # 报单引用
            'UserID': OrderField.UserID,  # 用户代码
            'OrderPriceType': OrderField.OrderPriceType,  # 报单价格条件
            'Direction': OrderField.Direction,  # 买卖方向
            'CombOffsetFlag': OrderField.CombOffsetFlag,  # 组合开平标志
            'CombHedgeFlag': OrderField.CombHedgeFlag,  # 组合投机套保标志
            'LimitPrice': OrderField.LimitPrice,  # 价格
            'VolumeTotalOriginal': OrderField.VolumeTotalOriginal,  # 数量
            'TimeCondition': OrderField.TimeCondition,  # 有效期类型
            'GTDDate': OrderField.GTDDate,  # GTD日期
            'VolumeCondition': OrderField.VolumeCondition,  # 成交量类型
            'MinVolume': OrderField.MinVolume,  # 最小成交量
            'ContingentCondition': OrderField.ContingentCondition,  # 触发条件
            'StopPrice': OrderField.StopPrice,  # 止损价
            'ForceCloseReason': OrderField.ForceCloseReason,  # 强平原因
            'IsAutoSuspend': OrderField.IsAutoSuspend,  # 自动挂起标志
            'BusinessUnit': OrderField.BusinessUnit,  # 业务单元
            'RequestID': OrderField.RequestID,  # 请求编号
            'OrderLocalID': OrderField.OrderLocalID,  # 本地报单编号
            'ExchangeID': OrderField.ExchangeID,  # 交易所代码
            'ParticipantID': OrderField.ParticipantID,  # 会员代码
            'ClientID': OrderField.ClientID,  # 客户代码
            'ExchangeInstID': OrderField.ExchangeInstID,  # 合约在交易所的代码
            'TraderID': OrderField.TraderID,  # 交易所交易员代码
            'InstallID': OrderField.InstallID,  # 安装编号
            'OrderSubmitStatus': OrderField.OrderSubmitStatus,  # 报单提交状态
            'NotifySequence': OrderField.NotifySequence,  # 报单提示序号
            'TradingDay': OrderField.TradingDay,  # 交易日
            'SettlementID': OrderField.SettlementID,  # 结算编号
            'OrderSysID': OrderField.OrderSysID,  # 报单编号
            'OrderSource': OrderField.OrderSource,  # 报单来源
            'OrderStatus': OrderField.OrderStatus,  # 报单状态
            'OrderType': OrderField.OrderType,  # 报单类型
            'VolumeTraded': OrderField.VolumeTraded,  # 今成交数量
            'VolumeTotal': OrderField.VolumeTotal,  # 剩余数量
            'InsertDate': OrderField.InsertDate,  # 报单日期
            'InsertTime': OrderField.InsertTime,  # 委托时间
            'ActiveTime': OrderField.ActiveTime,  # 激活时间
            'SuspendTime': OrderField.SuspendTime,  # 挂起时间
            'UpdateTime': OrderField.UpdateTime,  # 最后修改时间
            'CancelTime': OrderField.UpdateTime,  # 撤销时间
            'ActiveTraderID': OrderField.ActiveTraderID,  # 最后修改交易所交易员代码
            'ClearingPartID': OrderField.ClearingPartID,  # 结算会员编号
            'SequenceNo': OrderField.SequenceNo,  # 序号
            'FrontID': OrderField.FrontID,  # 前置编号
            'SessionID': OrderField.SessionID,  # 会话编号
            'UserProductInfo': OrderField.UserProductInfo,  # 用户端产品信息
            'StatusMsg': OrderField.StatusMsg,  # 状态信息
            'UserForceClose': OrderField.UserForceClose,  # 用户强评标志
            'ActiveUserID': OrderField.ActiveUserID,  # 操作用户代码
            'BrokerOrderSeq': OrderField.BrokerOrderSeq,  # 经纪公司报单编号
            'RelativeOrderSysID': OrderField.RelativeOrderSysID,  # 相关报单
            'ZCETotalTradedVolume': OrderField.ZCETotalTradedVolume,  # 郑商所成交数量
            'IsSwapOrder': OrderField.IsSwapOrder,  # 互换单标志
            'BranchID': OrderField.BranchID,  # 营业部编号
            'InvestUnitID': OrderField.InvestUnitID,  # 投资单元代码
            'AccountID': OrderField.AccountID,  # 资金账号
            'CurrencyID': OrderField.CurrencyID,  # 币种代码
            'IPAddress': OrderField.IPAddress,  # IP地址
            'MacAddress': OrderField.MacAddress  # Mac地址
        }
        self.__user.OnRtnOrder(Order)  # 转回调给User类的OnRtnOrder
        # 未调用API OrderInsert之前还未生成属性_PyCTP_Trader_API__rsp_OrderInsert
        # if hasattr(self, '_PyCTP_Trader_API__rsp_OrderInsert'):
        # 报单回报过滤1、套利系统的服务端或客户端发送的委托；2、字段SystemID不为空
        # self.__user.OnRtnOrder(Order)  # 转到user回调函数
        # for i in self.__user.get_list_strategy():  # 转到strategy回调函数
        #     if Order['OrderRef'][-2:] == i.get_strategy_id():
        #         i.OnRtnOrder(Order)
        # series_order = Series(Order)
        # self.__df_order = DataFrame.append(self.__df_order, other=series_order, ignore_index=True)

    def OnRtnTrade(self, TradeField):
        """成交通知"""
        # Trade = copy.deepcopy(Trade_input)
        # print(">>>PyCTP_Trade.OnRtnTrade() Trade =", Trade)
        # Trade = Utils.code_transform(Trade)
        # t = datetime.now().strftime("%Y-%m-%d %H:%M:%S %f")
        # Trade['time'] = t
        # if Utils.PyCTP_Trade_API_print:
        #     print('PyCTP_Trade.OnRtnTrade()', 'OrderRef:', Trade['OrderRef'], 'Time:', t, 'Trade:', Trade)
        Trade = {
            'BrokerID': TradeField.BrokerID,  # 经纪公司代码
            'InvestorID': TradeField.InvestorID,  # 投资者代码
            'InstrumentID': TradeField.InstrumentID,  # 合约代码
            'OrderRef': TradeField.OrderRef,  # 报单引用
            'UserID': TradeField.UserID,  # 用户代码
            'ExchangeID': TradeField.ExchangeID,  # 交易所代码
            'TradeID': TradeField.TradeID,  # 成交编号
            'Direction': TradeField.Direction,  # 买卖方向
            'OrderSysID': TradeField.OrderSysID,  # 报单编号
            'ParticipantID': TradeField.ParticipantID,  # 会员代码
            'ClientID': TradeField.ClientID,  # 客户代码
            'TradingRole': TradeField.TradingRole,  # 交易角色
            'ExchangeInstID': TradeField.ExchangeInstID,  # 合约在交易所的代码
            'OffsetFlag': TradeField.OffsetFlag,  # 开平标志
            'HedgeFlag': TradeField.HedgeFlag,  # 投机套保标志
            'Price': TradeField.Price,  # 价格
            'Volume': TradeField.Volume,  # 数量
            'TradeDate': TradeField.TradeDate,  # 成交时期
            'TradeTime': TradeField.TradeTime,  # 成交时间
            'TradeType': TradeField.TradeType,  # 成交类型
            'PriceSource': TradeField.PriceSource,  # 成交价来源
            'TraderID': TradeField.TraderID,  # 交易所交易员代码
            'OrderLocalID': TradeField.OrderLocalID,  # 本地报单编号
            'ClearingPartID': TradeField.ClearingPartID,  # 结算会员编号
            'BusinessUnit': TradeField.BusinessUnit,  # 业务单元
            'SequenceNo': TradeField.SequenceNo,  # 序号
            'TradingDay': TradeField.TradingDay,  # 交易日
            'SettlementID': TradeField.SettlementID,  # 结算编号
            'BrokerOrderSeq': TradeField.BrokerOrderSeq,  # 经纪公司报单编号
            'TradeSource': TradeField.TradeSource  # 成交来源
        }
        self.__user.OnRtnTrade(Trade)  # 转到user回调函数
        # for i in self.__user.get_list_strategy():  # 转到strategy回调函数
        #     if Trade['OrderRef'][-2:] == i.get_strategy_id():
        #         i.OnRtnTrade(Trade)
        # series_trade = Series(Trade)
        # self.__df_trade = DataFrame.append(self.__df_trade, other=series_trade, ignore_index=True)

    # 将order和trade记录保存到本地
    def save_df_order_trade(self):
        str_user_id = self.__UserID.decode()
        str_time = datetime.now().strftime("%Y-%m-%d %H%M%S")
        order_file_path = "data/order_" + str_user_id + "_" + str_time + '.csv'
        trade_file_path = "data/trade_" + str_user_id + "_" + str_time + '.csv'
        qry_order_file_path = "data/qry_order_" + str_user_id + "_" + str_time + '.csv'
        qry_trade_file_path = "data/qry_trade_" + str_user_id + "_" + str_time + '.csv'
        print(">>> PyCTP_Trade.save_df_order_trade() order_file_path =", order_file_path)
        print(">>> PyCTP_Trade.save_df_order_trade() trade_file_path =", trade_file_path)
        # self.__df_order.to_csv(order_file_path)
        # self.__df_trade.to_csv(trade_file_path)
        self.__df_qry_order.to_csv(qry_order_file_path)
        self.__df_qry_trade.to_csv(qry_trade_file_path)

    def OnErrRtnOrderAction(self, OrderActionField, RspInfo):
        """ 报单操作错误回报 """
        if RspInfo is not None:
            RspInfo = Utils.code_transform(RspInfo)
        if OrderActionField is not None:
            # OrderAction = Utils.code_transform(OrderAction)
            OrderAction = {
                'BrokerID': OrderActionField.BrokerID,  # 经纪公司代码
                'InvestorID': OrderActionField.InvestorID,  # 投资者代码
                'OrderActionRef': OrderActionField.OrderActionRef,  # 报单操作引用
                'OrderRef': OrderActionField.OrderRef,  # 报单引用
                'RequestID': OrderActionField.RequestID,  # 请求编号
                'FrontID': OrderActionField.FrontID,  # 前置编号
                'SessionID': OrderActionField.SessionID,  # 会话编号
                'ExchangeID': OrderActionField.ExchangeID,  # 交易所代码
                'OrderSysID': OrderActionField.OrderSysID,  # 报单编号
                'ActionFlag': OrderActionField.ActionFlag,  # 操作标志
                'LimitPrice': OrderActionField.LimitPrice,  # 价格
                'VolumeChange': OrderActionField.VolumeChange,  # 数量变化
                'ActionDate': OrderActionField.ActionDate,  # 操作日期
                'ActionTime': OrderActionField.ActionTime,  # 操作时间
                'TraderID': OrderActionField.TraderID,  # 交易所交易员代码
                'InstallID': OrderActionField.InstallID,  # 安装编号
                'OrderLocalID': OrderActionField.OrderLocalID,  # 本地报单编号
                'ActionLocalID': OrderActionField.ActionLocalID,  # 操作本地编号
                'ParticipantID': OrderActionField.ParticipantID,  # 会员代码
                'ClientID': OrderActionField.ClientID,  # 客户代码
                'BusinessUnit': OrderActionField.BusinessUnit,  # 业务单元
                'OrderActionStatus': OrderActionField.OrderActionStatus,  # 报单操作状态
                'UserID': OrderActionField.UserID,  # 用户代码
                'StatusMsg': OrderActionField.StatusMsg,  # 状态信息
                'InstrumentID': OrderActionField.InstrumentID,  # 合约代码
                'BranchID': OrderActionField.BranchID,  # 营业部编号
                'InvestUnitID': OrderActionField.InvestUnitID,  # 投资单元代码
                'IPAddress': OrderActionField.IPAddress,  # IP地址
                'MacAddress': OrderActionField.MacAddress  # Mac地址
            }
            dict_strategy = self.__user.get_dict_strategy()
            for strategy_id in dict_strategy:  # 转到strategy回调函数
                if OrderAction['OrderRef'][-2:] == dict_strategy[strategy_id].get_strategy_id():
                    dict_strategy[strategy_id].OnErrRtnOrderAction(OrderAction, RspInfo)
        if RspInfo is not None:
            RspInfo = Utils.code_transform(RspInfo)
        if Utils.PyCTP_Trade_API_print:
            print('PyCTP_Trade.OnErrRtnOrderAction()', 'OrderAction:', OrderAction, 'RspInfo:', RspInfo)
        #if not self.__rsp_OrderInsert['event'].is_set() and OrderAction['OrderActionStatus'] == PyCTP.THOST_FTDC_OST_Canceled:
        #    self.__rsp_OrderInsert['ErrorID'] = 79
        #    self.__rsp_OrderInsert['ErrorMsg'] = bytes('CTP:发送报单操作失败', 'gb2312')
        #    self.__rsp_OrderInsert['event'].set()

    def OnErrRtnOrderInsert(self, InputOrderField, RspInfo):
        """报单录入错误回报"""
        # print('PyCTP_Trade.OnErrRtnOrderInsert()', 'OrderRef:', InputOrder['OrderRef'], 'InputOrder:', InputOrder, 'RspInfo:', RspInfo)
        if InputOrderField is not None:
            # InputOrder = Utils.code_transform(InputOrder)
            InputOrder = {
                'BrokerID': InputOrderField.BrokerID,  # 经纪公司代码
                'InvestorID': InputOrderField.InvestorID,  # 投资者代码
                'InstrumentID': InputOrderField.InstrumentID,  # 合约代码
                'OrderRef': InputOrderField.OrderRef,  # 报单引用
                'UserID': InputOrderField.UserID,  # 用户代码
                'OrderPriceType': InputOrderField.OrderPriceType,  # 报单价格条件
                'Direction': InputOrderField.Direction,  # 买卖方向
                'CombOffsetFlag': InputOrderField.CombOffsetFlag,  # 组合开平标志
                'CombHedgeFlag': InputOrderField.CombHedgeFlag,  # 组合投机套保标志
                'LimitPrice': InputOrderField.LimitPrice,  # 价格
                'VolumeTotalOriginal': InputOrderField.VolumeTotalOriginal,  # 数量
                'TimeCondition': InputOrderField.TimeCondition,  # 有效期类型
                'GTDDate': InputOrderField.GTDDate,  # GTD日期
                'VolumeCondition': InputOrderField.VolumeCondition,  # 成交量类型
                'MinVolume': InputOrderField.MinVolume,  # 最小成交量
                'ContingentCondition': InputOrderField.ContingentCondition,  # 触发条件
                'StopPrice': InputOrderField.StopPrice,  # 止损价
                'ForceCloseReason': InputOrderField.ForceCloseReason,  # 强平原因
                'IsAutoSuspend': InputOrderField.IsAutoSuspend,  # 自动挂起标志
                'BusinessUnit': InputOrderField.BusinessUnit,  # 业务单元
                'RequestID': InputOrderField.RequestID,  # 请求编号
                'UserForceClose': InputOrderField.UserForceClose,  # 用户强评标志
                'IsSwapOrder': InputOrderField.IsSwapOrder,  # 互换单标志
                'ExchangeID': InputOrderField.ExchangeID,  # 交易所代码
                'InvestUnitID': InputOrderField.InvestUnitID,  # 投资单元代码
                'AccountID': InputOrderField.AccountID,  # 资金账号
                'CurrencyID': InputOrderField.CurrencyID,  # 币种代码
                'ClientID': InputOrderField.ClientID,  # 交易编码
                'IPAddress': InputOrderField.IPAddress,  # IP地址
                'MacAddress': InputOrderField.MacAddress  # Mac地址
            }
        # if RspInfo is not None:
        #     RspInfo = Utils.code_transform(RspInfo)
        # if Utils.PyCTP_Trade_API_print:
        #     print('PyCTP_Trade.OnErrRtnOrderInsert()', 'InputOrder:', InputOrder, 'RspInfo:', RspInfo)
        self.__user.OnErrRtnOrderInsert(InputOrder, RspInfo)  # 转到user回调函数
        # for i in self.__user.get_list_strategy():  # 转到strategy回调函数
        #     if InputOrder['OrderRef'][-2:] == i.get_strategy_id():
        #         i.OnErrRtnOrderInsert(InputOrder, RspInfo)

    def OnRtnTradingNotice(self, TradingNoticeInfoField):
        """ 交易通知 """
        if TradingNoticeInfoField is not None:
            TradingNoticeInfo = {
                'BrokerID':  TradingNoticeInfoField.BrokerID,  # 经纪公司代码
                'InvestorID':  TradingNoticeInfoField.InvestorID,  # 投资者代码
                'SendTime':  TradingNoticeInfoField.SendTime,  # 发送时间
                'FieldContent':  TradingNoticeInfoField.FieldContent,  # 消息正文
                'SequenceSeries':  TradingNoticeInfoField.SequenceSeries,  # 序列系列号
                'SequenceNo':  TradingNoticeInfoField.SequenceNo  # 序列号
            }

    dfInstrumentStatus = DataFrame()  # 保存InstrumentStatus的全局变量
    dfInstrument = DataFrame()  # 保存Instrument的全局变量

    def OnRtnExecOrder(self, ExecOrderField):
        """执行宣告通知"""
        ExecOrder = {
            'BrokerID': ExecOrderField.BrokerID,  # 经纪公司代码
            'InvestorID': ExecOrderField.InvestorID,  # 投资者代码
            'InstrumentID': ExecOrderField.InstrumentID,  # 合约代码
            'ExecOrderRef': ExecOrderField.ExecOrderRef,  # 执行宣告引用
            'UserID': ExecOrderField.UserID,  # 用户代码
            'Volume': ExecOrderField.Volume,  # 数量
            'RequestID': ExecOrderField.RequestID,  # 请求编号
            'BusinessUnit': ExecOrderField.BusinessUnit,  # 业务单元
            'OffsetFlag': ExecOrderField.OffsetFlag,  # 开平标志
            'HedgeFlag': ExecOrderField.HedgeFlag,  # 投机套保标志
            'ActionType': ExecOrderField.ActionType,  # 执行类型
            'PosiDirection': ExecOrderField.PosiDirection,  # 保留头寸申请的持仓方向
            'ReservePositionFlag': ExecOrderField.ReservePositionFlag,  # 期权行权后是否保留期货头寸的标记
            'CloseFlag': ExecOrderField.CloseFlag,  # 期权行权后生成的头寸是否自动平仓
            'ExecOrderLocalID': ExecOrderField.ExecOrderLocalID,  # 本地执行宣告编号
            'ExchangeID': ExecOrderField.ExchangeID,  # 交易所代码
            'ParticipantID': ExecOrderField.ParticipantID,  # 会员代码
            'ClientID': ExecOrderField.ClientID,  # 客户代码
            'ExchangeInstID': ExecOrderField.ExchangeInstID,  # 合约在交易所的代码
            'TraderID': ExecOrderField.TraderID,  # 交易所交易员代码
            'InstallID': ExecOrderField.InstallID,  # 安装编号
            'OrderSubmitStatus': ExecOrderField.OrderSubmitStatus,  # 执行宣告提交状态
            'NotifySequence': ExecOrderField.NotifySequence,  # 报单提示序号
            'TradingDay': ExecOrderField.TradingDay,  # 交易日
            'SettlementID': ExecOrderField.SettlementID,  # 结算编号
            'ExecOrderSysID': ExecOrderField.ExecOrderSysID,  # 执行宣告编号
            'InsertDate': ExecOrderField.InsertDate,  # 报单日期
            'InsertTime': ExecOrderField.InsertTime,  # 插入时间
            'CancelTime': ExecOrderField.CancelTime,  # 撤销时间
            'ExecResult': ExecOrderField.ExecResult,  # 执行结果
            'ClearingPartID': ExecOrderField.ClearingPartID,  # 结算会员编号
            'SequenceNo': ExecOrderField.SequenceNo,  # 序号
            'FrontID': ExecOrderField.FrontID,  # 前置编号
            'SessionID': ExecOrderField.SessionID,  # 会话编号
            'UserProductInfo': ExecOrderField.UserProductInfo,  # 用户端产品信息
            'StatusMsg': ExecOrderField.StatusMsg,  # 状态信息
            'ActiveUserID': ExecOrderField.ActiveUserID,  # 操作用户代码
            'BrokerExecOrderSeq': ExecOrderField.BrokerExecOrderSeq,  # 经纪公司报单编号
            'BranchID': ExecOrderField.BranchID,  # 营业部编号
            'InvestUnitID': ExecOrderField.InvestUnitID,  # 投资单元代码
            'AccountID': ExecOrderField.AccountID,  # 资金账号
            'CurrencyID': ExecOrderField.CurrencyID,  # 币种代码
            'IPAddress': ExecOrderField.IPAddress,  # IP地址
            'MacAddress': ExecOrderField.MacAddress  # Mac地址
        }
        print('OnRtnExecOrder()', ExecOrder)

    def OnRtnInstrumentStatus(self, InstrumentStatusField):
        """合约交易状态通知"""
        InstrumentStatus = {
            'ExchangeID': InstrumentStatusField.ExchangeID,  # 交易所代码
            'ExchangeInstID': InstrumentStatusField.ExchangeInstID,  # 合约在交易所的代码
            'SettlementGroupID': InstrumentStatusField.SettlementGroupID,  # 结算组代码
            'InstrumentID': InstrumentStatusField.InstrumentID,  # 合约代码
            'InstrumentStatus': InstrumentStatusField.InstrumentStatus,  # 合约交易状态
            'TradingSegmentSN': InstrumentStatusField.TradingSegmentSN,  # 交易阶段编号
            'EnterTime': InstrumentStatusField.EnterTime,  # 进入本状态时间
            'EnterReason': InstrumentStatusField.EnterReason  # 进入本状态原因
        }
        # 将查询结果用df保存
        series_InstrumentStatus = Series(InstrumentStatus)
        PyCTP_Trader_API.dfInstrumentStatus = pd.DataFrame.append(PyCTP_Trader_API.dfInstrumentStatus,
                                                                  other=series_InstrumentStatus,
                                                                  ignore_index=True)

    # 设置user为成员变量
    def set_user(self, user):
        self.__user = user

    # 将strategy实例的list设置为本类属性，在strategy实例中实现OnRtnXxx的回调函数
    def set_list_strategy(self, list_strategy):
        self.__list_strategy = list_strategy

    # 获取前置编号
    def get_front_id(self):
        return self.__FrontID

    # 获取会话编号
    def get_session_id(self):
        return self.__SessionID

    # 获取交易日
    def get_TradingDay(self):
        return self.__TradingDay

    # 为了解决错误而实现的函数
    # AttributeError: 'PyCTP_Trader_API' object has no attribute 'OnRspError'
    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        print("PyCTP_Trade.OnRspError() pRspInfo=", Utils.code_transform(pRspInfo), "nRequestID=", nRequestID, "bIsLast=", bIsLast)
        return None

