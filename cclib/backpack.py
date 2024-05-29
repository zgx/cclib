
import hashlib
import hmac
import urllib
from typing import Union
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError
from urllib.parse import urljoin
from cclib import errors, http_session
from datetime import datetime, timedelta


DEFAULT_BASE_URL = "https://api.backpack.exchange"

class BackpackApi:

    def __init__(self, api_key=None, secret_key=None, host=None):
        self.api_key = api_key
        self.secret_key = secret_key
        if host is None:
            self.host = DEFAULT_BASE_URL
        self.__session = http_session.get_session(self.host)

    def get_assets(self) -> list:
        """
        Retrieves the account balance.
        """
        uri = "/api/v1/assets"
        return self.request("GET", uri, auth=True)

    def get_markets(self) -> list:
        """
        Retrieves all the markets that are supported by the exchange.
        """
        uri = "/api/v1/markets"
        return self.request("GET", uri)

    def get_ticker(self, symbol: str) -> dict:
        """
        Retrieves the ticker for a specific symbol.
        """
        uri = "/api/v1/ticker"
        params = {"symbol": symbol}
        return self.request("GET", uri, params=params)

    def get_candles(self, symbol, interval, start_time: datetime, end_time=None) -> list:
        """
        Retrieves the candles for a specific symbol.
        """
        uri = "/api/v1/klines"
        start_ts = int(start_time.timestamp())

        params = {"symbol": symbol, "interval": interval, "startTime": start_ts}
        if end_time:
            end_ts = int(end_time.timestamp())
            params["endTime"] = end_ts
        return self.request("GET", uri, params=params)

    def get_depth(self, symbol):
        """
        Retrieves the order book depth for a given market symbol.
        """
        uri = "/api/v1/depth"
        params = {"symbol": symbol}
        return self.request("GET", uri, params=params)

    def _sign(self, data: dict) -> str:
        data = urllib.parse.urlencode(data)
        return hmac.new(self.secret_key.encode(), data.encode(), hashlib.sha256).hexdigest()

    def request(self, method: str, path: str, params: dict = None, data: dict = None, headers: dict = None, auth=False, timeout: int = 10) -> dict | list:
        if params is None:
            params = {}
        if data is None:
            data = {}
        if headers is None:
            headers = {}
        url = urljoin(self.host, path)
        if auth:
            params["api_key"] = self.api_key
            params["timestamp"] = int(datetime.now().timestamp())
            params["sign"] = self._sign(params)
        try:
            if method == "GET":
                response = self.__session.get(url, params=params, headers=headers, timeout=timeout)
            elif method == "POST":
                response = self.__session.post(url, params=params, json=data, headers=headers, timeout=timeout)
            else:
                raise errors.InvalidMethod(method)
            response.raise_for_status()
            return response.json()
        except Timeout as e:
            raise errors.TimeoutError(e)
        except RequestException as e:
            raise errors.ConnectionError(e)
        except Exception as e:
            raise errors.ExchangeError(e)