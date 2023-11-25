
import hmac
import base64
import urllib
import hashlib
import requests
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError
from urllib.parse import urljoin
from cclib import errors, http_session
from datetime import datetime

DEFAULT_BASE_URL = "https://api.hbdm.com"
BASE_URL_CN = "https://api.btcgateway.pro"
BASE_URL_AWS = "http://api.hbdm.vn"


class HuobiApiBase(object):

    def __init__(self, access_key="", secret_key="", base_url=DEFAULT_BASE_URL, request_session=None):
        self.base_url = base_url
        self._access_key = access_key
        self._secret_key = secret_key
        self.__host_url = urllib.parse.urlparse(self.base_url).hostname.lower()
        if request_session:
            self.__session = request_session
        else:
            self.__session = http_session.get_session(self.base_url)

    def set_base_url(self, base_url):
        self.base_url = base_url

    def heartbeat(self):
        return self._get("/heartbeat/")

    def _get(self, uri, params=None, headers=None, auth=False):
        return self.request('GET', uri, params, headers=headers, auth=auth)

    def _post(self, uri, params=None, body=None, headers=None, auth=False):
        return self.request('POST', uri, params, body, headers, auth)

    def request(self,method, uri, params=None, body=None, headers=None, auth=False):
        if uri.startswith("http://") or uri.startswith("https://"):
            url = uri
        else:
            url = urljoin(self.base_url, uri)

        if auth:
            timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            params = params if params else {}
            params.update({"AccessKeyId": self._access_key,
                           "SignatureMethod": "HmacSHA256",
                           "SignatureVersion": "2",
                           "Timestamp": timestamp})
            params["Signature"] = self.generate_signature(method, params, uri)
        if not headers:
            headers = {}
        if method == "GET":
            headers["Content-type"] = "application/x-www-form-urlencoded"
        elif method == "POST":
            headers["Accept"] = "application/json"
            headers["Content-type"] = "application/json"
        else:
            raise ValueError("unsupported method: {}".format(method))

        try:
            # async with aiohttp.ClientSession() as sess:
            #     rsp = await sess.request(method, url, params=params, data=body, headers=headers, timeout=10)
            rsp = self.__session.request(method, url, params=params, data=body, headers=headers, timeout=10)
            # rsp = requests.request(method, url, params=params, data=body, headers=headers, timeout=10)
        except ConnectionError as e:
            raise errors.ConnectionError from e
        except TimeoutError as e:
            raise errors.TimeoutError from e
        except RequestException as e:
            raise errors.NetworkError from e

        # if rsp.status_code == 200:
        #     rsp_obj = rsp.json()
        #     return rsp_obj
        # else:
        try:
            rsp_obj = rsp.json()
        except Exception as e:
            raise errors.ExchangeError(rsp.status_code, rsp.reason, status_code=rsp.status_code)
        if isinstance(rsp_obj, dict):
            if "status" in rsp_obj:
                s = rsp_obj['status']
                if s == "ok":
                    return rsp_obj
                elif s == 'maintain':
                    err_msg = rsp_obj.get('error', "exchange in maintain")
                    raise errors.ExchangeInMaintain(error_msg=err_msg, error_code="maintain", status_code=rsp.status_code, payload=rsp_obj)
                else:
                    err_msg = rsp_obj.get('err-msg', "")
                    if err_msg == "":
                        err_msg = rsp_obj.get('err_msg', 'unknown error')
                    raise errors.ExchangeError(err_msg, s, status_code=rsp.status_code, payload=rsp_obj)
            if "error" in rsp_obj:
                err_code = rsp.status_code
                err_msg = rsp_obj['error']
                raise errors.ExchangeError(err_msg, err_code, payload=rsp_obj)
        else:
            raise errors.ExchangeError(payload=rsp_obj)

    def generate_signature(self, method, params, request_path):
        if request_path.startswith("http://") or request_path.startswith("https://"):
            host_url = urllib.parse.urlparse(request_path).hostname.lower()
            request_path = '/' + '/'.join(request_path.split('/')[3:])
        else:
            host_url = self.__host_url  # urllib.parse.urlparse(self._host).hostname.lower()
        sorted_params = sorted(params.items(), key=lambda d: d[0], reverse=False)
        encode_params = urllib.parse.urlencode(sorted_params)
        payload = [method, host_url, request_path, encode_params]
        payload = "\n".join(payload)
        payload = payload.encode(encoding="UTF8")
        secret_key = self._secret_key.encode(encoding="utf8")
        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature


class HuobiUsdtSwapApi(HuobiApiBase):

    def get_contract_info(self):
        query_path = "/linear-swap-api/v1/swap_contract_info"
        return self._get(query_path)

    def get_candle(self, symbol, start_time: datetime, end_time: datetime, period='1min'):
        query_path = "/linear-swap-ex/market/history/kline"
        ts_from = int(start_time.timestamp())
        ts_to = int(end_time.timestamp())
        params = {"contract_code": symbol, "period": period, "from": ts_from, "to": ts_to}
        return self._get(query_path, params)

    def get_recent_candle(self, symbol, count, period="1min"):
        """
        获取最近count数量的k线
        """
        query_path = "/linear-swap-ex/market/history/kline"
        params = {"contract_code": symbol, "period": period, "size": count}
        return self._get(query_path, params)

    def get_index(self, symbol):
        query_path = "/linear-swap-api/v1/swap_index"
        return self._get(query_path, {"contract_code": symbol})

    def get_account_info(self, contract_code=None):
        uri = "/linear-swap-api/v1/swap_cross_account_info"
        body = {}
        if contract_code:
            body["contract_code"] = contract_code
        return self.request("POST", uri, body=body, auth=True)

    def get_position(self, contract_code=None):
        uri = "/linear-swap-api/v1/swap_cross_position_info"
        body = {}
        if contract_code:
            body["contract_code"] = contract_code
        return self.request("POST", uri, body=body, auth=True)


class HuobiFuturesApi(HuobiApiBase):
    def get_contract_info(self):
        query_path = "/api/v1/contract_contract_info"
        return self._get(query_path)

    def get_candle(self, symbol, start_time: datetime, end_time: datetime, period='1min'):
        query_path = "/market/history/kline"
        ts_from = int(start_time.timestamp())
        ts_to = int(end_time.timestamp())
        params = {"symbol": symbol, "period": period, "from": ts_from, "to": ts_to}
        return self._get(query_path, params)

    def get_recent_candle(self, symbol, count, period="1min"):
        """
        获取最近count数量的k线
        """
        uri = "/market/history/kline"
        params = {"symbol": symbol, "period": period, "size": count}
        return self._get(uri, params)

    def get_account_info(self, underlying=None):
        uri = "/api/v1/contract_account_info"
        body = {}
        if underlying:
            body['symbol'] = underlying
        return self.request("POST", uri, auth=True)

    def get_position(self):
        uri = "/api/v1/contract_position_info"
        return self.request("POST", uri, auth=True)


class HuobiCoinSwapApi(HuobiApiBase):
    def get_contract_info(self):
        query_path = "/swap-api/v1/swap_contract_info"
        return self._get(query_path)

    def get_candle(self, symbol, start_time: datetime, end_time: datetime, period='1min'):
        query_path = "/swap-ex/market/history/kline"
        ts_from = int(start_time.timestamp())
        ts_to = int(end_time.timestamp())
        params = {"symbol": symbol, "period": period, "from": ts_from, "to": ts_to}
        return self._get(query_path, params)

    def get_recent_candle(self, symbol, count, period="1min"):
        """
        获取最近count数量的k线
        """
        query_path = "/swap-ex/market/history/kline"
        params = {"symbol": symbol, "period": period, "size": count}
        return self._get(query_path, params)

    def get_index(self, symbol):
        query_path = "/index/market/history/swap_mark_price_kline"
        return self._get(query_path, {"contract_code": symbol})

    def get_account_info(self, contract_code=None):
        uri = "/swap-api/v1/swap_account_info"
        body = {}
        if contract_code:
            body["contract_code"] = contract_code
        return self.request("POST", uri, body=body, auth=True)

    def get_position(self, contract_code=None):
        uri = "/swap-api/v1/swap_position_info"
        body = {}
        if contract_code:
            body["contract_code"] = contract_code
        return self.request("POST", uri, body=body, auth=True)
