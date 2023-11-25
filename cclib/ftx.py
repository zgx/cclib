import hmac

from cclib import errors, http_session
import requests
import urllib
from urllib.parse import urljoin
from datetime import datetime

DEFAULT_BASE_URL = "https://ftx.com/"


class FtxApi:
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

    def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        if uri.startswith("https://") or uri.startswith("http://"):
            url = uri
        else:
            url = urljoin(self.base_url, uri)
        if not headers:
            headers = {}
        if not params:
            params = {}
        req = requests.Request(method, url, params=params, data=body, headers=headers)
        prepared_req = req.prepare()
        if auth:
            self.signature(prepared_req)
        try:
            # rsp = self.__session.request(method, url, params=params, data=body, headers=headers, timeout=10)
            rsp = self.__session.send(prepared_req)
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
                success = rsp_obj.get('success', False)
                code = 0 if success else -1
                msg = rsp_obj.get('error', 'unknown error')
                if success:
                    # 如果success为true，尽管status_code不是200，也直接返回
                    return rsp_obj
            else:
                code = -1
                msg = "response is not valid json obj:" + rsp_obj.dumps()
        except Exception as e:
            rsp_obj = None
            code = -1
            msg = "parse message json error. content:" + rsp.content.decode(encoding='utf8')

        if status_code == 429:
            raise errors.OutOfRateLimitError("请求超限：" + msg, code, status_code, payload=rsp_obj)
        raise errors.ExchangeError(msg, code, status_code, payload=rsp_obj)

    def signature(self, prepared):
        ts = int(datetime.now().timestamp() * 1000)
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        signature = hmac.new(self._secret_key.encode(), signature_payload, 'sha256').hexdigest()

        prepared.headers['FTX-KEY'] = self._access_key
        prepared.headers['FTX-SIGN'] = signature
        prepared.headers['FTX-TS'] = str(ts)
        if self._sub_account:
            prepared.headers['FTX-SUBACCOUNT'] = self._sub_account
        return signature

    def get_markets(self):
        """
        获取说有代码的市场信息
        :return:
        """
        uri = "/api/markets"
        return self.request("GET", uri)

    def get_single_market(self, symbol):
        uri = "/api/markets/" + symbol
        return self.request("GET", uri)

    def get_orderbook(self, symbol, depth=20):
        uri = "/api/markets/{}/orderbook".format(symbol)
        params = {"depth": depth}
        return self.request("GET", uri, params)

    def get_candles(self, symbol, start_time: datetime=None, end_time: datetime=None, resolution=60):
        """
        获取历史K线
        :param symbol:
        :param start_time:
        :param end_time:
        :param resolution: 窗口时长（秒）。选项：15, 60, 300, 900, 3600, 14400, 86400, or any multiple of 86400 up to 30*86400
        :return:
        """
        uri = "/api/markets/{}/candles".format(symbol)

        params = {"resolution": resolution}  #, 'start_time': start_time.timestamp(), 'end_time': end_time.timestamp()}
        if start_time:
            params['start_time'] = start_time.timestamp()
        if end_time:
            params['end_time'] = end_time.timestamp()
        return self.request("GET", uri, params)

    def get_account(self):
        uri = "/api/account"
        return self.request("GET", uri, auth=True)

    def get_balances(self):
        uri = "/api/wallet/balances"
        return self.request("GET", uri, auth=True)

    def get_positions(self):
        uri = "/api/positions"
        return self.request("GET", uri, auth=True)