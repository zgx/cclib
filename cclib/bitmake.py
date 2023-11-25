from datetime import datetime
from urllib.parse import urljoin
from requests import RequestException

from . import errors
from .http_session import get_session


class BitmakeApi:

    BASE_URL = "https://api.bitmake.com/"

    def __init__(self):
        self.__session = get_session(self.BASE_URL)

    def get_base_info(self):
        uri = "/t/v1/info"
        return self.request('GET', uri)

    def get_symbols(self):
        uri = "/u/v1/base/symbols"
        return self.request('GET', uri)

    def get_index(self, symbol=None):
        uri = "/t/v1/quote/index"
        params = {'symbol': symbol} if symbol else None
        return self.request('GET', uri, params)

    def get_candle(self, symbol, end_time: datetime = None, period='1m', limit=100):
        """

        :param symbol:
        :param end_time:
        :param period:  (1m,5m,15m,30m,1h,1d,1w,1M)
        :param limit:
        :return:
        """

        uri = "/t/v1/quote/klines"
        params = {'symbol': symbol, 'interval': period}
        if end_time:
            params['to'] = int(end_time.timestamp() * 1000)
        if limit:
            params['limit'] = limit
        return self.request('GET', uri, params=params)

    def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        if auth:
            raise NotImplementedError("未实现认证功能")

        if uri.startswith("https://") or uri.startswith("http://"):
            url = uri
        else:
            url = urljoin(self.BASE_URL, uri)

        if not headers:
            headers = {}

        headers["Content-type"] = "application/json"
        try:
            rsp = self.__session.request(method, url, params=params, data=body, headers=headers, timeout=10)
        except ConnectionError as e:
            raise errors.ConnectionError from e
        except TimeoutError as e:
            raise errors.TimeoutError from e
        except RequestException as e:
            raise errors.NetworkError from e

        status_code = rsp.status_code
        try:
            rsp_obj = rsp.json()
            if status_code == 200:
                return rsp_obj
            if isinstance(rsp_obj, dict):
                code = rsp_obj.get('code', -1)
                msg = rsp_obj.get('msg', 'unknown')
            else:
                code = -1
                msg = "response is not valid json obj:" + rsp_obj.dumps()
        except Exception as e:
            raise errors.ExchangeError("parse response json error:{}".format(e), -1, status_code=rsp.status_code, payload=rsp.content)

        if status_code == 429:
            raise errors.OutOfRateLimitError("请求超限：" + msg, code, status_code, payload=rsp_obj)
        else:
            raise errors.ExchangeError(msg, code, status_code, payload=rsp_obj)
