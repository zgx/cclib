import base64
import hashlib
import hmac
import json
import logging

import requests
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError
from urllib.parse import urljoin
import urllib

import cclib.http_session
from cclib import errors, http_session
from datetime import datetime, timedelta

DEFAULT_BASE_URL = "https://www.okx.com"
BASE_URL_CN = "https://www.okx.vip"
BASE_URL_AWS = "https://aws.okx.com"


class OkexApiBase(object):

    def __init__(self, access_key="", secret_key="", passphrase="",  base_url=DEFAULT_BASE_URL, request_session=None):
        self.base_url = base_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._passphrase = passphrase
        if request_session:
            self.__session = request_session
        else:
            self.__session = http_session.get_session(self.base_url)

    def api_version(self):
        raise NotImplementedError()

    def set_base_url(self, base_url):
        self.base_url = base_url

    def renew_session(self):
        self.__session = http_session.make_session()

    def _get(self, query_url, params=None):
        return self.request("GET", query_url, params)

    @staticmethod
    def __parse_params_to_str(params):
        url = '?'
        for key, value in params.items():
            url = url + str(key) + '=' + str(value) + '&'

        return url[0:-1]

    def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        if uri.startswith("https://") or uri.startswith("http://"):
            url = uri
        else:
            url = urljoin(self.base_url, uri)
        if not headers:
            headers = {}
        if params is None:
            params = {}
        if body and isinstance(body, dict):
            body = json.dumps(body)
        if auth:
            timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            request_path = uri + self.__parse_params_to_str(params)
            headers['OK-ACCESS-KEY'] = self._access_key
            headers['OK-ACCESS-TIMESTAMP'] = timestamp
            headers['OK-ACCESS-PASSPHRASE'] = self._passphrase
            sign = self.generate_signature(timestamp, method, request_path, body if body else "")
            headers['OK-ACCESS-SIGN'] = sign

        headers["Content-type"] = "application/json"
        try:
            rsp = self.__session.request(method, url, params=params, data=body, headers=headers, timeout=10)
        except ConnectionError as e:
            raise errors.ConnectionError from e
        except TimeoutError as e:
            raise errors.TimeoutError from e
        except RequestException as e:
            raise errors.NetworkError from e
        try:
            rsp_obj = rsp.json()
        except Exception as e:
            raise errors.ExchangeError("parse response json error:{}".format(e), -1, status_code=rsp.status_code, payload=rsp.content)
        if self.api_version() == 'v5':
            if isinstance(rsp_obj, dict):
                if "code" in rsp_obj:
                    rc = int(rsp_obj['code'])
                    msg = rsp_obj['msg']
                    if rc == 0:
                        return rsp_obj
                    if rc == 50002:
                        raise errors.TimeoutError(rc, msg)
                    if rc == 50001:
                        raise errors.ExchangeInMaintain(rc, msg)
                    if rc == 50011:
                        raise errors.OutOfRateLimitError(rc, msg)
                    raise errors.ExchangeError(error_msg=msg, error_code=rc, status_code=rsp.status_code, payload=rsp_obj)
                elif "msg" in rsp_obj:
                    err_code = rsp.status_code
                    err_msg = rsp_obj['msg']
                    raise errors.ExchangeError(err_msg, err_code, status_code=rsp.status_code, payload=rsp_obj)
                else:
                    raise errors.ExchangeError("未知的消息格式:{}".format(rsp_obj), error_code=-1, status_code=rsp.status_code, payload=rsp_obj)
            else:
                raise errors.ExchangeError("unknown data type:{}".format(rsp_obj), -1, rsp.status_code, payload=rsp_obj)
        elif self.api_version() == 'v3':
            if rsp.status_code == 200:
                return rsp_obj
            else:
                rc = rsp_obj.get('code', -1)
                msg = rsp_obj.get('error_message', "unknown error")
                raise errors.ExchangeError(msg, rc, status_code=rsp.status_code, payload=rsp_obj)

    def generate_signature(self, timestamp_s, method, request_path, body):
        payload = [timestamp_s, method, request_path, body]
        payload = ''.join(payload)
        payload = payload.encode(encoding="UTF8")
        secret_key = self._secret_key.encode(encoding="utf8")
        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature


class OkexApi(OkexApiBase):

    def api_version(self):
        return 'v5'

    def system_status(self):
        return self._get("/api/v5/system/status")

    def get_contract_info(self, instType, underlying=None, symbol = None):
        """
        请求参数
        参数名	类型	是否必须	描述
        instType	String	是	产品类型
        SPOT：币币
        SWAP：永续合约
        FUTURES：交割合约
        OPTION：期权
        uly	String	可选	合约标的指数，仅适用于交割/永续/期权，期权必填
        instId	String	否	产品ID
        返回结果

        {
            "code":"0",
            "msg":"",
            "data":[
            {
                "instType":"SWAP",
                "instId":"LTC-USD-SWAP",
                "uly":"LTC-USD",
                "category":"1",
                "baseCcy":"",
                "quoteCcy":"",
                "settleCcy":"LTC",
                "ctVal":"10",
                "ctMult":"1",
                "ctValCcy":"USD",
                "optType":"C",
                "stk":"",
                "listTime":"1597026383085",
                "expTime":"1597026383085",
                "lever":"10",
                "tickSz":"0.01",
                "lotSz":"1",
                "minSz":"1",
                "ctType":"linear",
                "alias":"this_week",
                "state":"live"
            }
        ]
        }
        返回参数
        参数名	类型	描述
        instType	String	产品类型
        instId	String	产品id， 如 BTC-USD-SWAP
        uly	String	合约标的指数，如 BTC-USD ，仅适用于交割/永续/期权
        category	String	手续费档位，每个交易产品属于哪个档位手续费
        baseCcy	String	交易货币币种，如 BTC-USDT 中的 BTC ，仅适用于币币
        quoteCcy	String	计价货币币种，如 BTC-USDT 中的USDT ，仅适用于币币
        settleCcy	String	盈亏结算和保证金币种，如 BTC 仅适用于交割/永续/期权
        ctVal	String	合约面值 ，仅适用于交割/永续/期权
        ctMult	String	合约乘数 ，仅适用于交割/永续/期权
        ctValCcy	String	合约面值计价币种，仅适用于交割/永续/期权
        optType	String	期权类型，C或P 仅适用于期权
        stk	String	行权价格 ，仅适用于期权
        listTime	String	上线日期 ，仅适用于交割 和 期权
        Unix时间戳的毫秒数格式，如 1597026383085
        expTime	String	交割日期 仅适用于交割 和 期权
        Unix时间戳的毫秒数格式，如 1597026383085
        lever	String	杠杆倍数 ， 不适用于币币，用于区分币币和币币杠杆
        tickSz	String	下单价格精度， 如 0.0001
        lotSz	String	下单数量精度， 如 BTC-USDT-SWAP：1
        minSz	String	最小下单数量
        ctType	String	linear：正向合约
        inverse：反向合约
        仅交割/永续有效
        alias	String	合约日期别名
        this_week：本周
        next_week：次周
        quarter：季度
        next_quarter：次季度
        仅适用于交割
        state	String	产品状态
        live：交易中
        suspend：暂停中
        preopen：预上线
        """
        query_path = "/api/v5/public/instruments"
        params = {"instType": instType}
        if underlying is not None:
            params['uly'] = underlying
        if symbol is not None:
            params['instId'] = symbol
        return self._get(query_path, params=params)

    def get_tickers(self, inst_type, uly=None, inst_id=None):
        """
        获取所有交易产品行情信息
        获取行情数据。获取可用于交易的所有交易产品的最新成交价、买一价、卖一价和24交易量。

        限速： 20次/2s
        HTTP请求
        GET /api/v5/market/tickers

        请求示例

        GET /api/v5/market/tickers?instType=SPOT
        请求参数
        参数名	类型	是否必须	描述
        instType	String	是	产品类型
            SPOT：币币
            SWAP：永续合约
            FUTURES：交割合约
            OPTION：期权
        uly	String	可选	合约标的指数，仅适用于交割/永续/期权，期权必填
        instId	String	否	产品ID

        返回参数
        参数名	类型	描述
        instType	String	产品类型
        instId	String	产品id， 如 BTC-USDT-SWAP
        last	String	最新成交价
        lastSz	String	最新成交的数量
        askPx	String	卖一价
        askSz	String	卖一价对应的量
        bidPx	String	买一价
        bidSz	String	买一价对应的量
        open24h	String	24小时开盘价
        high24h	String	24小时最高价
        low24h	String	24小时最低价
        volCcy24h	String	24小时成交量按币种折算
        vol24h	String	24小时成交量
        ts	String	系统时间戳
        """
        query_path = "/api/v5/market/tickers"
        params = {"instType": inst_type}
        if uly is not None:
            params['uly'] = uly
        if inst_id is not None:
            params['instId'] = inst_id
        return self._get(query_path, params=params)

    def get_candle(self, symbol, start_time: datetime = None, end_time: datetime = None, period='1m', limit=100):
        """
        获取所有交易产品K线数据
        获取K线数据。K线数据按请求的粒度分组返回，K线数据每个粒度最多可获取最近1440条。

        限速： 20次/2s
        HTTP请求
        GET /api/v5/market/candles

        请求示例

        GET /api/v5/market/candles?instId=BTC-USD-190927-5000-C
        请求参数
        参数名	类型	是否必须	描述
        instId	String	是	产品ID，如BTC-USD-190927-5000-C
        after	String	否	请求此ID之前（更旧的数据）的分页内容，传的值为对应接口的ts
        before	String	否	请求此ID之后（更新的数据）的分页内容，传的值为对应接口的ts
        bar	String	否	时间粒度，默认值1m
        如 [1m/3m/5m/15m/30m/1H/2H/4H/6H/12H/1D/1W/1M/3M/6M/1Y]
        limit	String	否	分页返回的结果集数量，最大为100，不填默认返回100条
        返回结果

        {
            "code":"0",
            "msg":"",
            "data":[
            [
                "1597026383085",
                "3.721",
                "3.743",
                "3.677",
                "3.708",
                "8422410",
                "22698348.04828491"
            ],
            [
                "1597026383085",
                "3.731",
                "3.799",
                "3.494",
                "3.72",
                "24912403",
                "67632347.24399722"
            ]
            ]
        }
        返回参数
        参数名	类型	描述
        ts	String	数据生成的时间，Unix时间戳的毫秒数格式，如 1597026383085
        o	String	开盘价格
        h	String	最高价格
        l	String	最低价格
        c	String	收盘价格
        vol	String	交易量（按张折算）
        volCcy	String	交易量（按币折算）
        """
        query_path = "/api/v5/market/candles"
        
        # 由于时间范围是开区间，前后各自延长一毫秒改为闭区间
        params = {"instId": symbol, "bar": period}
        if start_time:
            ts_from = int(start_time.timestamp()) * 1000 - 1
            params["before"] = ts_from
        if end_time:
            ts_to = int(end_time.timestamp()) * 1000 + 1
            params["after"] = ts_to
        if limit:
            params["limit"] = limit
        return self._get(query_path, params)

    def get_recent_candle(self, symbol, count, period="1m"):
        """
        获取最近count数量的k线
        """
        query_path = "/api/v5/market/candles"
        params = {"instId": symbol, "bar": period, "limit": count}
        return self._get(query_path, params)

    def get_history_candle(self, symbol, start_time: datetime = None, end_time: datetime = None, period='1m', limit=100):
        query_path = "/api/v5/market/history-candles"
        # 由于时间范围是开区间，前后各自延长一毫秒改为闭区间


        params = {"instId": symbol, "bar": period, "limit": limit}
        if start_time:
            ts_from = int(start_time.timestamp()) * 1000 - 1
            params['before'] = ts_from
        if end_time:
            ts_to = int(end_time.timestamp()) * 1000 + 1
            params['after'] = ts_to
        return self._get(query_path, params)

    def get_index(self, symbol, start_time: datetime, end_time: datetime, period='1min'):
        """
        获取指数K线数据
        指数K线数据每个粒度最多可获取最近1440条。

        限速： 20次/2s
        HTTP请求
        GET /api/v5/market/index-candles

        请求示例

        GET /api/v5/market/index-candles?instId=BTC-USD
        请求参数
        参数名	类型	是否必须	描述
        instId	String	是	现货指数，如BTC-USD
        after	String	否	请求此时间戳之前（更旧的数据）的分页内容，传的值为对应接口的ts
        before	String	否	请求此时间戳之后（更新的数据）的分页内容，传的值为对应接口的ts
        bar	String	否	时间粒度，默认值1m
        如 [1m/3m/5m/15m/30m/1H/2H/4H/6H/12H/1D/1W/1M/3M/6M/1Y]
        limit	String	否	分页返回的结果集数量，最大为100，不填默认返回100条
        返回结果

        {
            "code":"0",
            "msg":"",
            "data":[
            [
                "1597026383085",
                "3.721",
                "3.743",
                "3.677",
                "3.708"
            ],
            [
                "1597026383085",
                "3.731",
                "3.799",
                "3.494",
                "3.72"
            ]
            ]
        }
        返回参数
        参数名	类型	描述
        ts	String	开始时间，Unix时间戳的毫秒数格式，如 1597026383085
        o	String	开盘价格
        h	String	最高价格
        l	String	最低价格
        c	String	收盘价格
        返回的第一条K线数据可能不是完整周期k线，返回值数组顺分别为是：[ts,o,h,l,c]
        """
        query_path = "/api/v5/market/index-candles"
        ts_from = int(start_time.timestamp())
        ts_to = int(end_time.timestamp())
        params = {"instId": symbol, "bar": period, "before": ts_from, "after": ts_to}
        return self._get(query_path, params)

    def get_account_info(self, ccy=None):
        uri = "/api/v5/account/balance"
        params = {}
        if ccy:
            params['ccy'] = ccy
        return self.request('GET', uri, params, auth=True)

    def get_trade_fee(self, inst_type, inst_id=None, uly=None):
        """
        获取当前账户交易手续费费率
        """
        uri = "/api/v5/account/trade-fee"
        params = {'instType': inst_type}
        if inst_id:
            params['instId'] = inst_id
        if uly:
            params['uly'] = uly
        return self.request('GET', uri, params, auth=True)

    def get_position(self, inst_type=None, inst_id=None, pos_ids=None):
        uri = '/api/v5/account/positions'
        params = {}
        if inst_type:
            params['instType'] = inst_type
        if inst_id:
            params['instId'] = inst_id
        if pos_ids:
            params['posId'] = pos_ids
        return self.request('GET', uri, params, auth=True)

    def get_funding_rate_history(self, symbol, start_time=None, end_time=None):
        query_path = "/api/v5/public/funding-rate-history"
        # 由于时间范围是开区间，前后各自延长一毫秒改为闭区间
        params = {"instId": symbol}
        if start_time is not None:
            ts_from = int(start_time.timestamp()) * 1000 - 1
            params['before'] = ts_from
        if end_time is not None:
            ts_to = int(end_time.timestamp()) * 1000 + 1
            params['after'] = ts_to
        return self._get(query_path, params)

    def get_orders_history(self, inst_type):
        uri = "/api/v5/trade/orders-history"
        params = {"instType": inst_type}
        return self.request('GET', uri, params, auth=True)

    def get_fills(self, inst_type=None):
        uri = "/api/v5/trade/fills"
        params = {}
        if inst_type:
            params['instType'] = inst_type
        return self.request('GET', uri, params, auth=True)

    def rebase_info(self, api_key, broker_type=""):
        """

        :param apiKey: 用户的API key
        :return:
        """
        uri = "/api/v5/broker/fd/if-rebate"
        params = {'apiKey': api_key}
        if broker_type:
            params['brokerType'] = broker_type
        return self.request('GET', uri, params, auth=True)

    def generate_rebase_pre_orders(self, begin: str, end: str, broker_type=""):
        """

        :param begin: 	起始日期，（格式: YYYYMMdd，例如："20210623"）
        :param end: 	结束日期 格式: YYYYMMdd，例如："20210626"）
        :param broker_type: 	经纪商类型: api, oauth
        :return:
        """
        uri = "/api/v5/broker/fd/rebate-per-orders"
        params = {'begin': begin, 'end': end}
        if broker_type:
            params['brokerType'] = broker_type
        return self.request('POST', uri, body=params, auth=True)

    def get_rebase_pre_orders(self, query_type: bool = True):
        """

        :param type: 筛选条件类型，true： 获取当前用户所有已生成的历史记录 false：查询指定的历史记录
        :return:
        """
        uri = "/api/v5/broker/fd/rebate-per-orders"
        params = {'type': query_type}
        return self.request('GET', uri, params, auth=True)

    def send_order(self, inst_id, td_mode, side, ord_type, sz, ccy=None, cl_ord_id=None, tag=None, px=None, reduce_only=None, tgtCcy=None, posSide=None, triggerPx=None, ordId=None):
        """
        下单
        :param inst_id:
        :param td_mode:
        :param side:
        :param ord_type:
        :param sz:
        :param ccy:
        :param cl_ord_id:
        :param tag:
        :param px:
        :param reduce_only:
        :param tgtCcy:
        :param posSide:
        :param triggerPx:
        :param ordId:
        :return:
        """
        uri = "/api/v5/trade/order"
        params = {"instId": inst_id, "tdMode": td_mode, "side": side, "ordType": ord_type, "sz": sz}
        if ccy:
            params['ccy'] = ccy
        if cl_ord_id:
            params['clOrdId'] = cl_ord_id
        if tag:
            params['tag'] = tag
        if px:
            params['px'] = px
        if reduce_only:
            params['reduceOnly'] = reduce_only
        if tgtCcy:
            params['tgtCcy'] = tgtCcy
        if posSide:
            params['posSide'] = posSide
        if triggerPx:
            params['triggerPx'] = triggerPx
        if ordId:
            params['ordId'] = ordId
        return self.request('POST', uri, body=params, auth=True)

    def amend_order(self, order_id, new_size, new_price=None):
        """
        修改订单
        :param order_id:
        :param new_size:
        :param new_price:
        :return:
        """
        uri = "/api/v5/trade/amend-order"
        params = {"ordId": order_id, "sz": new_size}
        if new_price:
            params['px'] = new_price
        return self.request('POST', uri, body=params, auth=True)

    def get_account_bills(self, inst_type=None, ccy=None, type=None, sub_type=None):
        """
        获取账单流水
        :param inst_type:
        :param ccy:
        :param type:
        :param sub_type:
        :return:
        """
        uri = "/api/v5/account/bills"
        params = {}
        if inst_type:
            params['instType'] = inst_type
        if ccy:
            params['ccy'] = ccy
        if type:
            params['type'] = type
        if sub_type:
            params['subType'] = sub_type
        return self.request('GET', uri, params, auth=True)

    def get_easy_convert_assets(self):
        """
        获取小币一键兑换主流币币种列表。仅可兑换余额在 $10 以下小币币种。
        :return:
        """
        uri = "/api/v5/trade/easy-convert-currency-list"
        return self.request('GET', uri, auth=True)

    def easy_convert(self, from_ccy, to_ccy):
        """
        小币一键兑换
        :param from_ccy:
        :param to_ccy:
        :param amt:
        :return:
        """
        uri = "/api/v5/trade/easy-convert"
        params = {"fromCcy": from_ccy, "toCcy": to_ccy}
        return self.request('POST', uri, body=params, auth=True)

    def get_leverage_info(self, inst_id, mgn_mode="cross"):
        """
        获取杠杆倍数
        :param inst_id:
        :param mgn_mode:
        :return:
        """
        uri = "/api/v5/account/leverage-info"
        params = {"instId": inst_id, "mgnMode": mgn_mode}
        return self.request('GET', uri, params, auth=True)

    def set_leverage(self, inst_id, lever, mgn_mode="cross"):
        """
        设置杠杆倍数
        :param inst_id:
        :param lever: 杠杆倍数
        :param mgn_mode:
        :return:
        """
        uri = "/api/v5/account/set-leverage"
        params = {"instId": inst_id, "lever": str(lever), "mgnMode": mgn_mode}
        return self.request('POST', uri, body=params, auth=True)


class OkexV3FuturesApi(OkexApiBase):

    def api_version(self):
        return 'v3'

    def get_position(self):
        uri = "/api/futures/v3/position"
        return self.request('GET', uri, auth=True)

    def get_account_info(self):
        uri = "/api/futures/v3/accounts"
        return self.request('GET', uri, auth=True)
