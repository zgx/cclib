import datetime
import hashlib
import hmac
import json
import time
from urllib.parse import urljoin
import urllib.parse
from cclib import errors, http_session
import requests

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
