# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 00:31:14 2016

@author: Zhuolin
"""

import sys
import threading
import time
import Utils
from pandas import Series, DataFrame
import pyctp
# import pandas as pd


class PyCTP_Market_API(pyctp.CThostFtdcMdSpi):
    TradingDay = ''
    TIMEOUT = 10

    __RequestID = 0
    __isLogined = False
    is_first_connect = True

    def __init__(self, front, broker_id, user_id, password):
        pyctp.CThostFtdcMdSpi.__init__(self)
        self.__front_address = front
        self.__broker_id = broker_id
        self.__user_id = user_id
        self.__password = password

    def __IncRequestID(self):
        """ 自增并返回请求ID """
        # self.__RequestID += 1
        return self.__RequestID

    def setInvestorID(self, InvestorID):
        self.__InvestorID = InvestorID
        return self.__InvestorID

    def Connect(self, frontAddr):
        """ 连接前置服务器 """
        # 创建流文件路劲
        s_tmp = (self.__front_address[6:])
        n_position = s_tmp.index(':')
        s_part1 = (s_tmp[:n_position])
        s_part2 = (s_tmp[n_position+1:])
        s_path = 'conn/md/' + s_part1 + '_' + s_part2 + '/'
        Utils.make_dirs(s_path)
        # 创建api对象
        self.api = pyctp.CThostFtdcMdApi_CreateFtdcMdApi(s_path, False, False)
        self.api.RegisterSpi(self)
        self.api.RegisterFront(frontAddr)
        self.api.Init()

        self.__rsp_Connect = dict(event=threading.Event())
        self.__rsp_Connect['event'].clear()

        self.is_firsttime_logged = True  # 第一次连接
        return 0 if self.__rsp_Connect['event'].wait(self.TIMEOUT) else -4

    def UnConnect(self):
        """ywy：断开连接，释放MarketApi"""
        # self.RegisterSpi(None)
        self.api.Release()

    def Login(self, BrokerID, UserID='', Password=''):
        # print(">>> PyCTP_Market.Login() called, BrokerID =", BrokerID, "UserID =", UserID, "Password =", Password)
        """ 用户登录请求 """
        # 行情登录过程中的UserID和Password可以为空
        # reqUserLogin = dict(BrokerID=BrokerID,
        #                     UserID=UserID,
        #                     Password=Password)
        # 行情登录api方法形参结构体
        field = pyctp.CThostFtdcReqUserLoginField()
        field.BrokerID = BrokerID
        field.UserID = UserID
        field.Password = Password

        # 第一次登录
        if PyCTP_Market_API.is_first_connect:
            print(">>> PyCTP_Market.Login() 第一次登录, BrokerID =", BrokerID, "UserID =", UserID, "Password =", Password)
            self.__rsp_Login = dict(event=threading.Event(),
                                    RequestID=self.__IncRequestID())
        # 非第一次登录，例如登录之后断线重连
        else:
            pass

        ret = self.api.ReqUserLogin(field, self.__rsp_Login['RequestID'])
        if ret == 0:
            self.__rsp_Login['event'].clear()
            if self.__rsp_Login['event'].wait(self.TIMEOUT):
                if self.__rsp_Login['ErrorID'] == 0:
                    self.__isLogined = True
                    self.__BrokerID = BrokerID
                    self.__UserID   = UserID
                    self.__Password = Password
                else:
                    sys.stderr.write(str(self.__rsp_Login['ErrorMsg'], encoding='gb2312'))
                return self.__rsp_Login['ErrorID']
            else:
                return -4
        return ret

    def Logout(self):
        """ 登出请求 """
        # reqUserLogout = dict(BrokerID=self.__BrokerID,
        #                      UserID=self.__UserID)
        field = pyctp.CThostFtdcUserLogoutField()
        field.BrokerID = self.__BrokerID
        field.UserID = self.__UserID
        self.__rsp_Logout = dict(event=threading.Event(),
                                 RequestID=self.__IncRequestID())

        ret = self.ReqUserLogout(field, self.__rsp_Logout['RequestID'])
        if ret == 0:
            self.__rsp_Logout['event'].clear()
            if self.__rsp_Logout['event'].wait(self.TIMEOUT):
                if self.__rsp_Logout['ErrorID'] == 0:
                    self.__isLogined = False
                return self.__rsp_Logout['ErrorID']
            else:
                return -4
        return ret

    def SubMarketData(self, list_InstrumentID):
        print(">>>PyCTP_Market.SubMarketData() list_InstrumentID =", list_InstrumentID)
        """ 订阅行情 """
        self.__rsp_SubMarketData = dict(results=[], ErrorID=0, event=threading.Event(), RequestID=self.__IncRequestID())
        ret = self.api.SubscribeMarketData(list_InstrumentID, len(list_InstrumentID))
        # print(">>>PyCTP_Market.SubMarketData() ret =", ret)
        if ret == 0:
            self.__rsp_SubMarketData['event'].clear()
            if self.__rsp_SubMarketData['event'].wait(self.TIMEOUT):
                if self.__rsp_SubMarketData['ErrorID'] != 0:
                    return self.__rsp_SubMarketData['ErrorID']
                return self.__rsp_SubMarketData['results']
            else:
                return -4
        return ret

    def UnSubMarketData(self, InstrumentID):
        print(">>>PyCTP_Market.UnSubMarketData() InstrumentID =", InstrumentID)
        """ 退订行情 """
        self.__rsp_UnSubMarketData = dict(results=[], ErrorID=0, event=threading.Event(), RequestID=self.__IncRequestID())
        ret = self.api.UnSubscribeMarketData(InstrumentID, len(InstrumentID))
        if ret == 0:
            self.__rsp_UnSubMarketData['event'].clear()
            if self.__rsp_UnSubMarketData['event'].wait(self.TIMEOUT):
                if self.__rsp_UnSubMarketData['ErrorID'] != 0:
                    return self.__rsp_UnSubMarketData['ErrorID']
                return self.__rsp_UnSubMarketData['results']
            else:
                return -4
        return ret

    def OnFrontConnected(self):
        """ 当客户端与交易后台建立起通信连接时（还未登录前），该方法被调用。 """
        self.__rsp_Connect['event'].set()
        # if not PyCTP_Market_API.is_first_connect:
        #     self.Login(self.__BrokerID, self.__UserID, self.__Password)
        # else:
        #     self.__rsp_Connect['event'].set()

        if not PyCTP_Market_API.is_first_connect:
            self.anew_sub_market()

    # 非第一次连接需要重新登录和订阅行情
    def anew_sub_market(self):
        from MarketManager import MarketManager
        time.sleep(1.0)
        print("anew_sub_market()重新登录行情", self.Login(self.__BrokerID, self.__UserID, self.__Password))
        time.sleep(1.0)
        print("anew_sub_market()重新订阅行情", self.SubMarketData(MarketManager.list_instrument_subscribed))

    def OnFrontDisconnected(self, nReason):
        """ 当客户端与交易后台通信连接断开时，该方法被调用。当发生这个情况后，API会自动重新连接，客户端可不做处理。
        nReason 错误原因
        0x1001 网络读失败  4097
        0x1002 网络写失败
        0x2001 接收心跳超时
        0x2002 发送心跳失败
        0x2003 收到错误报文
        """
        print("OnFrontDisconnected()行情断开连接,nReason 错误原因=", nReason, '网络读失败')
        # sys.stderr.write('前置连接中断: %s' % hex(nReason))
        # sys.stderr.flush()
        if nReason == 4097:
            print('OnFrontDisconnected()网络异常，设置is_first_connect = False')
            PyCTP_Market_API.is_first_connect = False
            # self.Login(self.__BrokerID, self.__UserID, self.__Password)

        # 登陆状态时掉线, 自动重登陆
        # if self.__isLogined:
        #    self.__Inst_Interval()
        #    sys.stderr.write('自动登陆: %d' % self.Login(self.__BrokerID, self.__UserID, self.__Password))

    def OnRspUserLogin(self, RspUserLogin, RspInfo, RequestID, IsLast):
        # print(">>>PyCTP_Market.OnRspUserLogin() RspUserLogin =", RspUserLogin, "RspInfo =", RspInfo, "RequestID =", RequestID, "IsLast =", IsLast)
        """ 登录请求响应 """
        if RequestID == self.__rsp_Login['RequestID'] and IsLast:
            #self.__BrokerID = RspUserLogin['BrokerID']
            #self.__UserID = RspUserLogin['UserID']
            self.__SystemName = RspUserLogin.SystemName
            # print(">>>PyCTP_Market.OnRspUserLogin() RspUserLogin.SystemName =", RspUserLogin.SystemName)
            self.__TradingDay = RspUserLogin.TradingDay
            PyCTP_Market_API.TradingDay = RspUserLogin.TradingDay  # 全局变量，记录交易日
            # print(">>>PyCTP_Market.OnRspUserLogin() RspUserLogin.TradingDay =", RspUserLogin.TradingDay)
            self.__SessionID = RspUserLogin.SessionID
            self.__MaxOrderRef = RspUserLogin.MaxOrderRef
            self.__INETime = RspUserLogin.INETime
            self.__FrontID = RspUserLogin.FrontID
            self.__FFEXTime = RspUserLogin.FFEXTime
            self.__SHFETime = RspUserLogin.SHFETime
            self.__CZCETime = RspUserLogin.CZCETime
            self.__DCETime = RspUserLogin.DCETime
            self.__LoginTime = RspUserLogin.LoginTime
            RspInfo = {
                'ErrorID': RspInfo.ErrorID,  # 错误代码
                'ErrorMsg': RspInfo.ErrorMsg  # 错误信息
            }
            self.__rsp_Login.update(RspInfo)
            self.__rsp_Login['event'].set()

    def get_TradingDay(self):
        return self.__TradingDay

    def OnRspUserLogout(self, RspUserLogout, RspInfo, RequestID, IsLast):
        """ 登出请求响应 """
        if RequestID == self.__rsp_Logout['RequestID'] and IsLast:
            self.__rsp_Logout.update(RspInfo)
            self.__rsp_Logout['event'].set()

    def OnRspError(self, RspInfo,  RequestID, IsLast):
        """ 错误信息 """
        sys.stderr.write(repr(([RspInfo.ErrorID, str(RspInfo.ErrorMsg, encoding='gb2312')], RequestID, IsLast)))

    def OnRspSubMarketData(self, SpecificInstrumentField, RspInfoField, RequestID, IsLast):
        """ 订阅行情应答 """
        # print(">>>PyCTP_Market.OnRspSubMarketData() called")
        if RequestID == self.__rsp_SubMarketData['RequestID']:
            if RspInfoField is not None:
                RspInfo = {
                    'ErrorID': RspInfoField.ErrorID,  # 错误代码
                    'ErrorMsg': RspInfoField.ErrorMsg  # 错误信息
                }
                self.__rsp_SubMarketData.update(RspInfo)
            if SpecificInstrumentField is not None:
                InstrumentId = SpecificInstrumentField.InstrumentID
                self.__rsp_SubMarketData['results'].append(InstrumentId)
            if IsLast:
                self.__rsp_SubMarketData['event'].set()

    def OnRspUnSubMarketData(self, SpecificInstrumentField, RspInfoField, RequestID, IsLast):
        """ 取消订阅行情应答 """
        if RequestID == self.__rsp_UnSubMarketData['RequestID']:
            if RspInfoField is not None:
                RspInfo = {
                    'ErrorID': RspInfoField.ErrorID,  # 错误代码
                    'ErrorMsg': RspInfoField.ErrorMsg  # 错误信息
                }
                self.__rsp_UnSubMarketData.update(RspInfo)
            if SpecificInstrumentField is not None:
                InstrumentID = SpecificInstrumentField.InstrumentID
                self.__rsp_UnSubMarketData['results'].append(InstrumentID)
            if IsLast:
                self.__rsp_UnSubMarketData['event'].set()

    def OnRtnDepthMarketData(self, DepthMarketData):
        """ 行情推送 """
        # print('PyCTP_Market.OnRtnDepthMarketData() DepthMarketData=', DepthMarketData)
        tick = {
            'TradingDay': DepthMarketData.TradingDay,
            'InstrumentID': DepthMarketData.InstrumentID,
            'ExchangeID': DepthMarketData.ExchangeID,
            'ExchangeInstID': DepthMarketData.ExchangeInstID,
            'LastPrice': DepthMarketData.LastPrice,
            'PreSettlementPrice': DepthMarketData.PreSettlementPrice,
            'PreClosePrice': DepthMarketData.PreClosePrice,
            'PreOpenInterest': DepthMarketData.PreOpenInterest,
            'OpenPrice': DepthMarketData.OpenPrice,
            'HighestPrice': DepthMarketData.HighestPrice,
            'LowestPrice': DepthMarketData.LowestPrice,
            'Volume': DepthMarketData.Volume,
            'Turnover': DepthMarketData.Turnover,
            'OpenInterest': DepthMarketData.OpenInterest,
            'ClosePrice': DepthMarketData.ClosePrice,
            'SettlementPrice': DepthMarketData.SettlementPrice,
            'UpperLimitPrice': DepthMarketData.UpperLimitPrice,
            'LowerLimitPrice': DepthMarketData.LowerLimitPrice,
            'UpdateTime': DepthMarketData.UpdateTime,
            'UpdateMillisec': DepthMarketData.UpdateMillisec,
            'BidPrice1': DepthMarketData.BidPrice1,
            'BidVolume1': DepthMarketData.BidVolume1,
            'AskPrice1': DepthMarketData.AskPrice1,
            'AskVolume1': DepthMarketData.AskVolume1,
            'BidPrice2': DepthMarketData.BidPrice2,
            'AveragePrice': DepthMarketData.AveragePrice,
            'ActionDay': DepthMarketData.ActionDay
        }
        # tick = Utils.code_transform(DepthMarketData)
        # print('PyCTP_Market.OnRtnDepthMarketData() tick=', tick)
        # PyCTP_Market_API.df_tick_data = PyCTP_Market_API.df_tick_data.append(other=Series(tick),
        #                                                                      ignore_index=True,
        #                                                                      verify_integrity=False)
        # 遍历策略列表，并将tick转发给策略类对应处理方法
        # for i in self.__list_strategy:
        #     i.OnRtnDepthMarketData(tick)

        self.__market_manager.OnRtnDepthMarketData(tick)  # 转行情回调给MarketManager.OnRtnDepthMarketData()

    # 将strategy实例的list设置为本类属性，在strategy实例中实现行情推送回调函数
    def set_strategy(self, list_strategy):
        self.__list_strategy = list_strategy

    def get_strategy(self):
        return self.__list_strategy

    def set_MarketManager(self, obj):
        self.__market_manager = obj

