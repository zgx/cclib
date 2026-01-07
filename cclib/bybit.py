import datetime
import hashlib
import hmac
import json
import time
from urllib.parse import urljoin
import urllib.parse
from cclib import errors, http_session
import requests
from typing import Optional

DEFAULT_BASE_URL = "https://api.bybit.com"


class BaseBybitApi:

    def __init__(self, access_key="", secret_key="", sub_account="", base_url=None, request_session=None):
        self._access_key = access_key
        self._secret_key = secret_key
        self._sub_account = sub_account
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = DEFAULT_BASE_URL
        if request_session:
            self.__session = request_session
        else:
            self.__session = http_session.get_session(self.base_url)
        super().__init__()

    def request(self, method, uri, params=None, headers=None, auth=False):
        if uri.startswith("https://") or uri.startswith("http://"):
            url = uri
        else:
            url = urljoin(self.base_url, uri)
        if not headers:
            headers = {}
        if not params:
            params = {}

        if auth:
            if 'timestamp' not in params:
                params['timestamp'] = int(time.time() * 1000)
            if 'api_key' not in params:
                params['api_key'] = self._access_key
            sign = self.generate_signature(method, params)
            params['sign'] = sign

        if method != "GET":
            body = json.dumps(params)
            params = {}
        else:
            body = None

        try:
            rsp = self.__session.request(method, url, params=params, data=body, headers=headers, timeout=10)
        except ConnectionError as e:
            raise errors.ConnectionError from e
        except TimeoutError as e:
            raise errors.TimeoutError from e
        except requests.RequestException as e:
            raise errors.NetworkError from e

        status_code = rsp.status_code
        code = -1
        msg = 'unknown error'
        try:
            rsp_obj = rsp.json()
            if status_code == 200:
                return rsp_obj
            if isinstance(rsp_obj, dict):
                code = rsp_obj.get('ret_code', -1)
                msg = rsp_obj.get('ret_msg', 'unknown error')
                if code == 0:
                    # 如果返回成功，尽管status_code不是200，也直接返回
                    return rsp_obj
        except Exception as e:
            rsp_obj = None
            code = -1
            msg = "parse message json error. content:" + rsp.content.decode(encoding='utf8')

        if status_code == 403:
            raise errors.OutOfRateLimitError("请求超限：" + msg, code, status_code, payload=rsp_obj)
        if code == 10003 or code == 10018:
            raise errors.OutOfRateLimitError("请求超限：" + msg, code, status_code, payload=rsp_obj)
        raise errors.ExchangeError(msg, code, status_code, payload=rsp_obj)

    def generate_signature(self, method, params):
        sorted_params = sorted(params.items(), key=lambda d: d[0], reverse=False)
        encode_params = urllib.parse.urlencode(sorted_params)
        sign = hmac.new(self._secret_key.encode(), encode_params.encode(), digestmod=hashlib.sha256).hexdigest()
        return sign



class BybitV5Api:

    def __init__(self, access_key="", secret_key="", base_url=None, request_session=None):
        self._access_key = access_key
        self._secret_key = secret_key
        # self._sub_account = sub_account
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = DEFAULT_BASE_URL
        if request_session:
            self.__session = request_session
        else:
            self.__session = http_session.get_session(self.base_url)
        super().__init__()

    def request(self, method, uri, params=None, body="", headers=None, auth=False, recv_window=5000):
        if uri.startswith("https://") or uri.startswith("http://"):
            url = uri
        else:
            url = urljoin(self.base_url, uri)
        if not headers:
            headers = {}
        if not params:
            params = {}
        if not headers:
            headers = {}
        headers['Content-Type'] = 'application/json'
        query_string = urllib.parse.urlencode(params)
        if isinstance(body, dict):
            body = json.dumps(body)

        if auth:
            """
            需要簽名的接口必須包含以下http頭參數:

            X-BAPI-API-KEY - API密鑰
            X-BAPI-TIMESTAMP - UTC毫秒時間戳
            X-BAPI-SIGN - 請求參數簽名
            X-BAPI-RECV-WINDOW - 請求有效時間窗口（毫秒）
            X-Referer or Referer - 經紀商用戶專用的頭參數
            """

            timestamp = int(time.time() * 1000)
            recv_window_str = str(recv_window)
            headers['X-BAPI-API-KEY'] = self._access_key
            headers['X-BAPI-TIMESTAMP'] = str(timestamp)
            headers['X-BAPI-RECV-WINDOW'] = recv_window_str
            sign = self.generate_signature(method, timestamp, query_string, body, recv_window_str)
            headers['X-BAPI-SIGN'] = sign

        try:
            rsp = self.__session.request(method, url, params=query_string, data=body, headers=headers, timeout=10)
        except ConnectionError as e:
            raise errors.ConnectionError from e
        except TimeoutError as e:
            raise errors.TimeoutError from e
        except requests.RequestException as e:
            raise errors.NetworkError from e

        status_code = rsp.status_code
        code = -1
        msg = 'unknown error'
        try:
            # 外层通用数据结构
            # {
            #     "retCode": 0,
            #     "retMsg": "OK",
            #     "result": {
            #     },
            #     "retExtInfo": {},
            #     "time": 1671017382656
            # }
            rsp_obj = rsp.json()
            if status_code == 200:
                return rsp_obj
            if isinstance(rsp_obj, dict):
                code = rsp_obj.get('retCode', -1)
                msg = rsp_obj.get('retMsg', 'unknown error')
                if code == 0:
                    # 如果返回成功，尽管status_code不是200，也直接返回
                    return rsp_obj
        except Exception as e:
            rsp_obj = None
            code = -1
            msg = "parse message json error. content:" + rsp.content.decode(encoding='utf8')

        if status_code == 403:
            raise errors.OutOfRateLimitError("请求超限：" + msg, code, status_code, payload=rsp_obj)
        if code == 10003 or code == 10018:
            raise errors.OutOfRateLimitError("请求超限：" + msg, code, status_code, payload=rsp_obj)
        raise errors.ExchangeError(msg, code, status_code, payload=rsp_obj)

    def generate_signature(self, method, timestamp, query_string, body_string="", recv_window=""):
        # 拼接規則 (According to official V5 API documentation)
        # GET: timestamp+api_key+recv_window+queryString
        # POST: timestamp+api_key+recv_window+jsonBodyString

        if method == "GET":
            s = str(timestamp) + self._access_key + recv_window + query_string
        else:
            # POST also needs recv_window according to official documentation
            s = str(timestamp) + self._access_key + recv_window + body_string

        sign = hmac.new(self._secret_key.encode(), s.encode(), digestmod=hashlib.sha256).hexdigest()
        return sign
    
    def get_wallet_balance(self, account_type="UNIFIED", coin=None):
        """
        获取钱包余额
        :param account_type: 账户类型。统一账户: UNIFIED。经典账户: CONTRACT, SPOT
        :param coin: 币种
        :return:
        """
        uri = "/v5/account/wallet-balance"
        params = {'accountType': account_type}
        if coin:
            params['coin'] = coin
        return self.request('GET', uri, params, auth=True)

    def get_instruments(self, category, symbol=None, status=None, limit=1000, cursor=None):
        """
        获取合约信息
        :param category: 产品类型. spot,linear,inverse,option
        :param symbol: 合約名稱
        :param status: 状态。只用Trading状态
        :param limit: 每页数量
        :param cursor: 游标，用于翻页
        :return:
        """
        uri = "/v5/market/instruments-info"
        params = {'category': category, 'limit': limit}
        if symbol:
            params['symbol'] = symbol
        if status:
            params['status'] = status
        if cursor:
            params['cursor'] = cursor
        return self.request('GET', uri, params)
    
    def get_market_time(self):
        """
        获取服务器时间
        :return:
        """
        uri = "/v5/market/time"
        return self.request('GET', uri)
    
    def get_candle(self, symbol, category, interval="1", 
                   start_time: Optional[datetime.datetime]=None, end_time: Optional[datetime.datetime]=None, 
                   limit=None):
        """
        获取K线数据。
        :param symbol:	合约名称
        :param category: 产品类型. spot,linear,inverse. 默认linear
        :param interval: K线周期。 1 3 5 15 30 60 120 240 360 720 "D" "M" "W"
        :param limit: Default 200, max 200
        :return:
        """
        uri = "/v5/market/kline"
        params = {'symbol': symbol, 'interval': interval}
        if category:
            params['category'] = category
        if start_time is not None:
            start_ts = int(start_time.timestamp()) * 1000
            params['start'] = start_ts
        if end_time is not None:
            end_ts = int(end_time.timestamp()) * 1000
            params['end'] = end_ts
        if limit is not None:
            params['limit'] = limit
        return self.request('GET', uri, params)

    def get_tickers(self, category, symbol=None):
        uri = "/v5/market/tickers"
        params = {'category': category}
        if symbol:
            params['symbol'] = symbol
        return self.request('GET', uri, params)

    def get_position_info(self, category, symbol=None, base_coin=None, settle_coin=None, limit=None, cursor=None):
        """
        获取持仓信息
        Query real-time position data, such as position size, cumulative realized PNL, etc.
        
        :param category: 产品类型. linear, inverse, option
        :param symbol: 合约名称. 如果传入symbol，则返回数据无论是否有持仓
        :param base_coin: Base coin. option only. 如果不传baseCoin, 则返回全部期权的持仓
        :param settle_coin: Settle coin. 对于linear & inverse, settleCoin 或 symbol 必传一个. 如果都不传，默认返回 USDT perpetual
        :param limit: 每页数量限制. [1, 200]. Default: 20
        :param cursor: Cursor. Used for pagination
        :return: Response object containing position information
        """
        uri = "/v5/position/list"
        params = {'category': category}
        if symbol:
            params['symbol'] = symbol
        if base_coin:
            params['baseCoin'] = base_coin
        if settle_coin:
            params['settleCoin'] = settle_coin
        if limit is not None:
            params['limit'] = limit
        if cursor:
            params['cursor'] = cursor
        return self.request('GET', uri, params, auth=True)

    def set_leverage(self, category, symbol, buy_leverage, sell_leverage):
        """
        设置杠杆倍数
        Set the leverage for a trading pair
        
        :param category: 产品类型. linear, inverse
        :param symbol: 合约名称
        :param buy_leverage: 买入杠杆. [0, max leverage of corresponding risk limit]. 
                             注意: 在单向持仓模式下, buyLeverage必须等于sellLeverage
        :param sell_leverage: 卖出杠杆. [0, max leverage of corresponding risk limit].
                              注意: 在单向持仓模式下, buyLeverage必须等于sellLeverage
        :return: Response object
        """
        uri = "/v5/position/set-leverage"
        body = {
            'category': category,
            'symbol': symbol,
            'buyLeverage': str(buy_leverage),
            'sellLeverage': str(sell_leverage)
        }
        return self.request('POST', uri, body=body, auth=True)

    def switch_position_mode(self, category, mode, symbol=None, coin=None):
        """
        切换持仓模式
        Switch Position Mode. Supports switching the position mode for USDT perpetual and Inverse futures.
        
        :param category: 产品类型. linear, inverse
        :param mode: 持仓模式. 0: 单向持仓 (one-way/merged single), 3: 双向持仓 (hedge mode/both sides)
        :param symbol: 合约名称. symbol 或 coin 至少传一个. symbol优先级更高
        :param coin: Coin name. symbol 或 coin 至少传一个
        :return: Response object
        
        注意:
        - 单向模式 (mode=0): 只能在买入或卖出方向开一个仓位
        - 双向模式 (mode=3): 可以同时在买入和卖出方向开仓
        - Linear perpetual 支持一键切换全仓模式 (通过传入coin参数)
        """
        uri = "/v5/position/switch-mode"
        body = {
            'category': category,
            'mode': mode
        }
        if symbol:
            body['symbol'] = symbol
        if coin:
            body['coin'] = coin
        return self.request('POST', uri, body=body, auth=True)
