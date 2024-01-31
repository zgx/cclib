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

    def request(self, method, uri, params=None, body="", headers=None, auth=False):
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
            X-Referer or Referer - 經紀商用戶專用的頭參數
            """

            timestamp = int(time.time() * 1000)
            headers['X-BAPI-API-KEY'] = self._access_key
            headers['X-BAPI-TIMESTAMP'] = str(timestamp)
            sign = self.generate_signature(method, timestamp, query_string, body)
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
        # 拼接規則. 
        # GET timestamp+api_key+recv_window+queryString
        # POST timestamp+api_key+recv_window+bodyString

        if method == "GET":
            s = str(timestamp) + self._access_key + recv_window + query_string
        else:
            s = str(timestamp) + self._access_key + body_string

        # sorted_params = sorted(params.items(), key=lambda d: d[0], reverse=False)
        # encode_params = urllib.parse.urlencode(sorted_params)
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



class BybitSpotApi(BaseBybitApi):

    def get_symbols(self):
        uri = "/spot/v1/symbols"
        return self.request('GET', uri)

    def get_candles(self, symbol, start_time: datetime.datetime, end_time: datetime.datetime, interval='1m', limit=1000):
        """
        获取K线数据。
        :param symbol:	Name of the trading pair
        :param start_time:	Start time, 起始时间，在缺少endTime字段的情况是，此字段不起作用
        :param end_time: End time, unit in millisecond
        :param interval: candle interval. 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w, 1M
        :param limit: Default value is 1000, max 1000
        :return:
        """
        uri = "/spot/quote/v1/kline"
        params = {'symbol': symbol, 'interval': interval}
        if start_time is not None:
            start_ts = int(start_time.timestamp() * 1000)
            params['startTime'] = start_ts
        if end_time is not None:
            end_ts = int(end_time.timestamp() * 1000)
            params['endTime'] = end_ts
        if limit is not None:
            params['limit'] = limit
        return self.request('GET', uri, params)


class BybitCoinSwapApi(BaseBybitApi):

    def get_symbols(self):
        uri = "/v2/public/symbols"
        return self.request('GET', uri)

    def get_tickers(self):
        uri = "/v2/public/tickers"
        return self.request('GET', uri)

    def get_candles(self, symbol, start_time: datetime.datetime, interval="1", limit=200):
        """

        :param symbol:
        :param start_time: 起始时间
        :param limit:
        :param interval: Data refresh interval. Enum : 1 3 5 15 30 60 120 240 360 720 "D" "M" "W"
        :return:
        """
        uri = "/v2/public/kline/list"
        from_ts = int(start_time.timestamp())
        params = {'symbol': symbol, 'from': from_ts, 'interval': interval, 'limit': limit}
        return self.request('GET', uri, params)

    def get_balances(self, symbol=None):
        uri = "/v2/private/wallet/balance"
        params = {}
        if symbol:
            params['coin'] = symbol
        return self.request('GET', uri, params, auth=True)

    def get_positions(self, symbol=None):
        uri = "/v2/private/position/list"
        params = {}
        if symbol:
            params['coin'] = symbol
        return self.request('GET', uri, params, auth=True)
