import hashlib
import hmac
import urllib
from typing import Union
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError
from urllib.parse import urljoin
from cclib import errors, http_session
from datetime import datetime, timedelta

DEFAULT_BASE_URL_S = "https://api.binance.com"
DEFAULT_BASE_URL_F = "https://fapi.binance.com"  # U本位合约交易地址
DEFAULT_BASE_URL_D = "https://dapi.binance.com"  # 币本位合约交易地址


class BinanceApiBase(object):

    def __init__(self,  access_key="", secret_key="", base_url="", request_session=None):
        if base_url is None or base_url == "":
            raise ValueError("未指定base_url")
        self._access_key = access_key
        self._secret_key = secret_key
        self.base_url = base_url
        if request_session:
            self.__session = request_session
        else:
            self.__session = http_session.get_session(base_url)
        super().__init__()

    def _get(self, query_url, params=None):
        return self.request('GET', query_url, params)

    def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        if uri.startswith("https://") or uri.startswith("http://"):
            url = uri
        else:
            url = urljoin(self.base_url, uri)
        if not headers:
            headers = {}
        if params is None:
            params = {}
        if auth:
            timestamp = int(datetime.now().timestamp() * 1000)
            if 'recvWindows' not in params:
                params['recvWindow'] = 5000
            if 'timestamp' not in params:
                params['timestamp'] = str(timestamp)
            sign = self.generate_signature(params, body if body else "")
            headers['X-MBX-APIKEY'] = self._access_key
            params['signature'] = sign

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
            rsp_obj = None
            code = -1
            msg = "parse message json error. content:" + rsp.content.decode(encoding='utf8')

        if status_code == 429:
            raise errors.OutOfRateLimitWarning("即将超限:" + msg, code, status_code, payload=rsp_obj)
        if status_code == 418:
            raise errors.OutOfRateLimitError("已经超限" + msg, code, status_code, payload=rsp_obj)
        if -1099 <= code <= -1000:
            if code == -1003:  # TOO_MANY_REQUESTS 请求权重过多； 请使用websocket获取最新更新。
                raise errors.OutOfRateLimitError(msg, code, status_code, payload=rsp_obj)
            if code == -1007:  # -1007 TIMEOUT 等待后端服务器响应超时。 发送状态未知； 执行状态未知。
                raise errors.ServiceTimeout(msg, code, status_code, payload=rsp_obj)
            if code == -1022:  # -1022 INVALID_SIGNATURE 此请求的签名无效。
                raise errors.AuthenticationError(msg, code, status_code, payload=rsp_obj)
            if code == -1016:  # -1016 SERVICE_SHUTTING_DOWN 该服务不可用。
                raise errors.ExchangeInMaintain(msg, code, status_code, payload=rsp_obj)
            raise errors.ExchangeError(msg, code, status_code, payload=rsp_obj)
        if -1199 <= code <= -1100:
            raise errors.ArgumentsError(msg, code, status_code, payload=rsp_obj)
        if -2099 <= code <= -2000:  # Processing Issues
            if code in (-2014, -2015):
                raise errors.AuthenticationError(msg, code, status_code, payload=rsp_obj)
            raise errors.ExchangeError(msg, code, status_code, payload=rsp_obj)
        if -4099 <= code <= -4000:
            raise errors.ExchangeError(msg, code, status_code, payload=rsp_obj)
        raise errors.ExchangeError(msg, code, status_code, payload=rsp_obj)

    def generate_signature(self, params, body):
        # payload = [request_path, body]
        # payload = ''.join(payload)
        encode_params = urllib.parse.urlencode(params)
        payload = encode_params + body
        payload = payload.encode(encoding="UTF8")
        secret_key = self._secret_key.encode(encoding="utf8")
        signature = hmac.new(secret_key, payload, digestmod=hashlib.sha256).hexdigest()
        return signature

    def _get_candle(self, uri, symbol, start_time: datetime, end_time: datetime, limit=None, interval='1m', ):
        params = {'symbol': symbol, 'interval': interval}
        if start_time is not None:
            start_ts = start_time.timestamp()
            params['startTime'] = int(start_ts * 1000)
        if end_time is not None:
            end_ts = end_time.timestamp()
            params['endTime'] = int(end_ts * 1000)
        if limit is None and (start_time is not None and end_time is not None):
            limit = (end_time - start_time).total_seconds() // 60 + 1
        if limit is not None:
            params['limit'] = int(limit)
        return self.request("GET", uri, params)


class BinanceSApi(BinanceApiBase):
    """
    币安钱包、现货、杠杆、币安宝、矿池接口
    """
    def __init__(self, access_key="", secret_key="", base_url=None, request_session=None):
        if base_url is None or base_url == "":
            base_url = DEFAULT_BASE_URL_S
        super().__init__(access_key, secret_key, base_url, request_session)

    def system_status(self):
        uri = "/sapi/v1/system/status"
        return self.request("GET", uri)

    def get_exchange_info(self):
        query_path = "/api/v3/exchangeInfo"
        return self._get(query_path)
    
    def get_ticker_price(self, symbol: str=None):
        """
        获取最新价格
        :param symbol: 不发送交易对参数，则会返回所有交易对信息
        :return: 当发送交易对参数时，返回的结果为单个symbol的最新价格；当未发送交易对参数时，返回的结果为列表

        """
        query_path = "/api/v3/ticker/price"
        params = {}
        if symbol:
            params['symbol'] = symbol
        return self._get(query_path, params)

    def get_candle(self, symbol, start_time: Union[datetime, None], end_time: Union[datetime, None],limit=1000, interval='1m'):
        """

        :param symbol:
        :param start_time:
        :param end_time:
        :param interval: m -> 分钟; h -> 小时; d -> 天; w -> 周; M -> 月. 如：1m表示1分钟
        :param limit: 默认 500; 最大 1000.
        :return: K线列表
        如果未发送 startTime 和 endTime ，默认返回最近的交易。
        """
        uri = "/api/v3/klines"
        return self._get_candle(uri, symbol, start_time, end_time, limit, interval)

    def get_account_coins_config(self):
        """
        获取针对用户的所有(Binance支持充提操作的)币种信息。
        :return:
        """
        uri = "/sapi/v1/capital/config/getall"
        return self.request("GET", uri, auth=True)

    def get_account_info(self):
        uri = "/api/v3/account"
        return self.request("GET", uri, auth=True)

    def get_api_status(self):
        uri = "/sapi/v1/account/apiTradingStatus"
        return self.request("GET", uri, auth=True)

    def asset_transfer(self, transfer_type, asset, amount):
        """
        用户万向划转
        您需要开通api key 允许万向划转权限来调用此接口。
        目前支持的type划转类型:
        MAIN_C2C 现货钱包转向C2C钱包
        MAIN_UMFUTURE 现货钱包转向U本位合约钱包
        MAIN_CMFUTURE 现货钱包转向币本位合约钱包
        MAIN_MARGIN 现货钱包转向杠杆全仓钱包
        MAIN_MINING 现货钱包转向矿池钱包
        C2C_MAIN C2C钱包转向现货钱包
        C2C_UMFUTURE C2C钱包转向U本位合约钱包
        C2C_MINING C2C钱包转向矿池钱包
        UMFUTURE_MAIN U本位合约钱包转向现货钱包
        UMFUTURE_C2C U本位合约钱包转向C2C钱包
        UMFUTURE_MARGIN U本位合约钱包转向杠杆全仓钱包
        CMFUTURE_MAIN 币本位合约钱包转向现货钱包
        MARGIN_MAIN 杠杆全仓钱包转向现货钱包
        MARGIN_UMFUTURE 杠杆全仓钱包转向U本位合约钱包
        MINING_MAIN 矿池钱包转向现货钱包
        MINING_UMFUTURE 矿池钱包转向U本位合约钱包
        MINING_C2C 矿池钱包转向C2C钱包
        MARGIN_CMFUTURE 杠杆全仓钱包转向币本位合约钱包
        CMFUTURE_MARGIN 币本位合约钱包转向杠杆全仓钱包
        MARGIN_C2C 杠杆全仓钱包转向C2C钱包
        C2C_MARGIN C2C钱包转向杠杆全仓钱包
        MARGIN_MINING 杠杆全仓钱包转向矿池钱包
        MINING_MARGIN 矿池钱包转向杠杆全仓钱包
        MAIN_PAY 现货钱包转向支付钱包
        PAY_MAIN 支付钱包转向现货钱包
        :param transfer_type: 划转类型
        :param asset: 币种代码
        :param amount: 数量
        :return:
        """
        uri = "/sapi/v1/asset/transfer"
        params = {'type:': transfer_type, 'asset': asset, 'amount': amount}
        return self.request('POST', uri, params=params, auth=True)

    def get_asset_transfer_history(self, trans_type: str):
        """
        获取用户万向划转历史
        :return:
        """
        uri = "/sapi/v1/asset/transfer"
        params = {
            "type": trans_type
        }
        return self.request('GET', uri, params, auth=True)

    def transfer_with_futures(self, asset, amount, transfer_type):
        """
        执行现货账户与合约账户之间的划转
        :param asset:
        :param amount:
        :param transfer_type: 划转类型
            1: 现货账户向USDT合约账户划转
            2: USDT合约账户向现货账户划转
            3: 现货账户向币本位合约账户划转
            4: 币本位合约账户向现货账户划转
        :return:
        """
        uri = "/sapi/v1/futures/transfer"
        params = {'asset': asset, "amount": amount, "type": transfer_type}
        return self.request('POST', uri, params, auth=True)

    def get_open_orders(self, symbol=None):
        uri = "/api/v3/openOrders"
        params = {}
        if symbol:
            params['symbol'] = symbol
        return self.request('GET', uri, params, auth=True)

    def create_order(self, symbol, side, order_type, quantity=None, quote_order_qty=None, price=None, time_in_force=None, new_client_order_id=None):
        """
        创建订单
        :param symbol: 交易对
        :param side: BUY/SELL
        :param order_type: LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER
        :param quantity: 数量
        :param quote_order_qty: 报价订单数量(仅适用于MARKET订单)
        :param price: 价格
        :param time_in_force: GTC, IOC, FOK
        :param new_client_order_id:
        :return:
        """
        uri = "/api/v3/order"
        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
        }
        if quantity:
            params['quantity'] = quantity
        if quote_order_qty:
            params['quoteOrderQty'] = quote_order_qty
        if price:
            params['price'] = price
        if time_in_force:
            params['timeInForce'] = time_in_force
        if new_client_order_id:
            params['newClientOrderId'] = new_client_order_id

        return self.request('POST', uri, params, auth=True)

    def get_margin_pairs(self):
        uri = "/sapi/v1/margin/allPairs"
        return self.request('GET', uri, None, auth=True)

    def broker_if_new_user(self, broker_id):
        uri = "/sapi/v1/apiReferral/ifNewUser"
        params = {"apiAgentCode": broker_id}
        return self.request('GET', uri, params, auth=True)

    def get_recent_rebate(self, start_time: datetime, end_time: datetime, customer_id=""):
        uri = "/sapi/v1/apiReferral/rebate/recentRecord"
        params = {"startTime": int(start_time.timestamp() * 1000), "endTime": int(end_time.timestamp() * 1000)}
        if customer_id:
            params["customerId"] = customer_id
        return self.request('GET', uri, params, auth=True)

    def get_sub_main_transfer_history(self, asset=None, type=None, start_time=None, end_time=None, page=None, limit=None):
        """

        :param asset:
        :param type:
        :param start_time:
        :param end_time:
        :param page:
        :param limit:
        :return:
        """
        uri = "/sapi/v1/sub-account/transfer/subUserHistory"
        params = {}
        if asset:
            params['asset'] = asset
        if type:
            params['type'] = type
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time

        return self.request('GET', uri, params, auth=True)

    def get_income(self, income_type=None):
        """
        查询收益历史。
        :return:
        """
        uri = "/sapi/v1/income"
        params = {}
        if income_type:
            params['incomeType'] = income_type
        return self.request('GET', uri, params, auth=True)

    def get_dust(self):
        """
        获取可以转换成 BNB 的小额资产列表.。
        :return:
        """
        uri = "/sapi/v1/asset/dust-btc"
        return self.request('POST', uri, None, auth=True)

    def asset_dust(self, asset):
        """
        把小额资产转换成 BNB.
        :param asset:
        :return:
        """
        uri = "/sapi/v1/asset/dust"
        params = {'asset': asset}
        return self.request('POST', uri, params, auth=True)


class BinanceFApi(BinanceApiBase):
    """
    币安U本位API
    """

    def __init__(self, access_key="", secret_key="", base_url=None, request_session=None):
        if base_url is None or base_url == "":
            base_url = DEFAULT_BASE_URL_F
        super().__init__(access_key=access_key, secret_key=secret_key, base_url=base_url, request_session=request_session)

    def ping(self):
        query_path = "/fapi/v1/ping"
        return self._get(query_path)

    def get_server_time(self):
        query_path = "/fapi/v1/time"
        return self._get(query_path)
    
    def get_exchange_info(self):
        query_path = "/fapi/v1/exchangeInfo"
        return self._get(query_path)
    
    def get_ticker_price(self, symbol: str=None):
        """
        获取最新价格
        :param symbol: 不发送交易对参数，则会返回所有交易对信息
        :return: 当发送交易对参数时，返回的结果为单个symbol的最新价格；当未发送交易对参数时，返回的结果为列表

        """
        query_path = "/fapi/v2/ticker/price"
        params = {}
        if symbol:
            params['symbol'] = symbol
        return self._get(query_path, params)

    
    def get_candle(self, symbol, start_time: datetime, end_time: datetime, limit=None, interval='1m'):
        """
        获取一分钟K线列表，获取区间为: [start_time, end_time]
        """
        query_path = '/fapi/v1/klines'
        return self._get_candle(query_path, symbol, start_time, end_time, limit, interval)

    def get_funding_rate(self, symbol: str = None, start_time: datetime = None, end_time: datetime = None, limit = None):
        """
        查询资金费率历史。
        如果 startTime 和 endTime 都未发送, 返回最近 limit 条数据.
        如果 startTime 和 endTime 之间的数据量大于 limit, 返回 startTime + limit情况下的数据。
        :param symbol:
        :param start_time:
        :param end_time:
        :param limit: 默认值 100，最大值 1000
        :return:
        """
        query_path = '/fapi/v1/fundingRate'
        params = {}
        if symbol:
            params['symbol'] = symbol
        if start_time:
            start_ts = int(start_time.timestamp() * 1000)
            params['startTime'] = start_ts
        if end_time:
            end_ts = int(end_time.timestamp() * 1000)
            params['endTime'] = end_ts
        if limit:
            params['limit'] = limit
        return self.request('GET', query_path, params)
    
    def get_open_interest_hist(self, symbol: str = None, period: str = None, limit: int = None, start_time: datetime = None, end_time: datetime = None):
        """
        获取持仓量信息
        :param symbol: 交易对
        :param period: 时间周期
        :param limit: 默认值 30, 最大值 500
        :param start_time: 开始时间
        :param end_time: 结束时间
        :return:
        """
        query_path = '/futures/data/openInterestHist'
        params = {}
        if symbol:
            params['symbol'] = symbol
        if period:
            params['period'] = period
        if limit:
            params['limit'] = limit
        if start_time:
            start_ts = int(start_time.timestamp() * 1000)
            params['startTime'] = start_ts
        if end_time:
            end_ts = int(end_time.timestamp() * 1000)
            params['endTime'] = end_ts
        return self.request('GET', query_path, params)

    def get_account_balance(self):
        """
        获取账户余额
        :return:
        """
        uri = "/fapi/v2/balance"
        return self.request('GET', uri, auth=True)

    def get_position(self, symbol=None):
        """
        持仓信息
        :return:
        """
        uri = "/fapi/v2/positionRisk"
        params = {}
        if symbol:
            params['symbol'] = symbol
        return self.request('GET', uri, params, auth=True)

    def get_account_info(self):
        """
        获取资产信息和持仓信息
        :return:
        """
        uri = "/fapi/v2/account"
        return self.request('GET', uri, auth=True)

    def set_leverage(self, symbol, leverage):
        uri = "/fapi/v1/leverage"
        params = {"symbol": symbol, "leverage": leverage}
        return self.request('POST', uri, params, auth=True)

    def get_position_side_dual(self):
        """
        查询用户目前在 所有symbol 合约上的持仓模式：双向持仓或单向持仓。
        响应：
        {
            "dualSidePosition": true // "true": 双向持仓模式；"false": 单向持仓模式
        }
        :return:
        """
        uri = "/fapi/v1/positionSide/dual"
        return self.request('GET', uri, auth=True)

    def set_position_side_dual(self, twoSidePosition: bool):
        """
        变换用户在 所有symbol 合约上的持仓模式：双向持仓或单向持仓。
        :param twoSidePosition:
        :return:
        """
        uri = "/fapi/v1/positionSide/dual"
        params = {"dualSidePosition": twoSidePosition}
        return self.request('POST', uri, params, auth=True)

    def set_multi_assets_margin(self, is_multi_assets_margin: bool):
        """
        变换用户在 所有symbol 合约上的联合保证金模式：开启或关闭联合保证金模式。
        :param is_multi_assets_margin "true": 联合保证金模式开启；"false": 联合保证金模式关闭
        :return:
        """
        uri = "/fapi/v1/multiAssetsMargin"
        params = {"multiAssetsMargin": is_multi_assets_margin}
        return self.request('POST', uri, params, auth=True)

    def get_multi_assets_margin(self):
        """
        查询用户目前在 所有symbol 合约上的联合保证金模式。
        :return:
        """
        uri = "/fapi/v1/multiAssetsMargin"
        return self.request('GET', uri, auth=True)

    def broker_if_new_user(self, broker_id):
        uri = "/fapi/v1/apiReferral/ifNewUser"
        params = {"brokerId": broker_id}
        return self.request('GET', uri, params, auth=True)

    def get_income(self, income_type=None):
        """
        查询收益历史。
        :return:
        """
        uri = "/fapi/v1/income"
        params = {}
        if income_type:
            params['incomeType'] = income_type
        return self.request('GET', uri, params, auth=True)

    def get_force_orders(self, symbol=None, auto_close_type=None, start_time: datetime = None, end_time: datetime = None, limit: int = None):
        """
        查询用户强平订单记录。未指定auto_close_type时返回强平和ADL订单。
        """
        uri = "/fapi/v1/forceOrders"
        params = {}
        if symbol:
            params['symbol'] = symbol
        if auto_close_type:
            params['autoCloseType'] = auto_close_type
        if start_time:
            params['startTime'] = int(start_time.timestamp() * 1000) if isinstance(start_time, datetime) else int(start_time)
        if end_time:
            params['endTime'] = int(end_time.timestamp() * 1000) if isinstance(end_time, datetime) else int(end_time)
        if limit:
            params['limit'] = int(limit)
        return self.request('GET', uri, params, auth=True)

    # def get_multi_assets_margin(self):
    #     """
    #     查询用户目前在 所有symbol 合约上的联合保证金模式。
    #     :return:
    #     """
    #     uri = "/fapi/v1/multiAssetsMargin"
    #     return self.request('GET', uri, auth=True)
    #
    # def set_multi_assets_margin(self):
    #     """
    #     变换用户在 所有symbol 合约上的联合保证金模式：开启或关闭联合保证金模式。
    #     :return:
    #     """
    #     uri = "/fapi/v1/multiAssetsMargin"
    #     return self.request('POST', uri, auth=True)


class BinanceDApi(BinanceApiBase):
    """
    币安币本位API
    """

    def __init__(self, access_key="", secret_key="", base_url=None, request_session=None):
        if base_url is None or base_url == "":
            base_url = DEFAULT_BASE_URL_D
        super().__init__(access_key=access_key, secret_key=secret_key, base_url=base_url, request_session=request_session)

    def ping(self):
        query_path = "/dapi/v1/ping"
        return self._get(query_path)

    def get_server_time(self):
        query_path = "/dapi/v1/time"
        return self._get(query_path)

    def get_exchange_info(self):
        query_path = "/dapi/v1/exchangeInfo"
        return self._get(query_path)
    
    def get_ticker_price(self, symbol: str=None):
        """
        获取最新价格
        :param symbol: 不发送交易对参数，则会返回所有交易对信息
        :return: 当发送交易对参数时，返回的结果为单个symbol的最新价格；当未发送交易对参数时，返回的结果为列表

        """
        query_path = "/dapi/v1/ticker/price"
        params = {}
        if symbol:
            params['symbol'] = symbol
        return self._get(query_path, params)

    def get_candle(self, symbol, start_time: datetime, end_time: datetime, limit=None, interval='1m'):
        """
        获取一分钟K线列表，获取区间为: [start_time, end_time]
        """
        query_path = '/dapi/v1/klines'
        return self._get_candle(query_path, symbol, start_time, end_time, limit, interval)

    def get_funding_rate(self, symbol: str = None, start_time: datetime = None, end_time: datetime = None, limit = None):
        """
        查询资金费率历史。
        如果 startTime 和 endTime 都未发送, 返回最近 limit 条数据.
        如果 startTime 和 endTime 之间的数据量大于 limit, 返回 startTime + limit情况下的数据。
        :param symbol:
        :param start_time:
        :param end_time:
        :param limit: 默认值 100，最大值 1000
        :return:
        """
        query_path = '/dapi/v1/fundingRate'
        params = {}
        if symbol:
            params['symbol'] = symbol
        if start_time:
            start_ts = int(start_time.timestamp() * 1000)
            params['startTime'] = start_ts
        if end_time:
            end_ts = int(end_time.timestamp() * 1000)
            params['endTime'] = end_ts
        if limit:
            params['limit'] = limit
        return self.request('GET', query_path, params)

    def get_account_balance(self):
        """
        获取账户余额
        :return:
        """
        uri = "/dapi/v1/balance"
        return self.request('GET', uri, auth=True)

    def get_position(self, marginAsset=None, pair=None):
        """
        持仓信息
        marginAsset 和 pair 不要同时提供
        marginAsset 和 pair 均不提供则返回所有上市状态和结算中的symbol
        对于单向持仓模式，仅会展示"BOTH"方向的持仓
        对于双向持仓模式，会展示所有"BOTH", "LONG", 和"SHORT"方向的持仓
        :return:
        """
        uri = "/dapi/v1/positionRisk"
        params = {}
        if marginAsset:
            params['marginAsset'] = marginAsset
        if pair:
            params['pair'] = pair
        return self.request('GET', uri, params, auth=True)

    def get_account_info(self):
        """
        获取资产信息和持仓信息
        :return:
        """
        uri = "/dapi/v1/account"
        return self.request('GET', uri, auth=True)

    def set_leverage(self, symbol, leverage):
        uri = "/dapi/v1/leverage"
        params = {"symbol": symbol, "leverage": leverage}
        return self.request('POST', uri, params, auth=True)

    def get_trades(self, symbol, start_time : datetime = None, end_time : datetime = None):
        uri = "/dapi/v1/userTrades"
        params = {"symbol": symbol}
        if start_time:
            start_ts = int(start_time.timestamp() * 1000)
            params["start_time"] = start_ts
        if end_time:
            end_ts = int(end_time.timestamp() * 1000)
            params["end_time"] = end_ts
        return self.request("GET", uri, params, auth=True)

    def get_income(self, income_type=None):
        uri = "/dapi/v1/income"
        params = {}
        if income_type:
            params['incomeType'] = income_type
        return self.request('GET', uri, params, auth=True)

    def get_force_orders(self, symbol=None, auto_close_type=None, start_time: datetime = None, end_time: datetime = None, limit: int = None):
        """
        查询用户强平历史，未指定auto_close_type时返回强平和ADL订单。
        """
        uri = "/dapi/v1/forceOrders"
        params = {}
        if symbol:
            params['symbol'] = symbol
        if auto_close_type:
            params['autoCloseType'] = auto_close_type
        if start_time:
            params['startTime'] = int(start_time.timestamp() * 1000) if isinstance(start_time, datetime) else int(start_time)
        if end_time:
            params['endTime'] = int(end_time.timestamp() * 1000) if isinstance(end_time, datetime) else int(end_time)
        if limit:
            params['limit'] = int(limit)
        return self.request('GET', uri, params, auth=True)


class BinancePApi(BinanceApiBase):
    """
    币安统一账户（Portfolio Margin）API
    文档: https://developers.binance.com/docs/derivatives/portfolio-margin/general-info
    """
    def __init__(self, access_key="", secret_key="", base_url="https://papi.binance.com", request_session=None):
        super().__init__(access_key=access_key, secret_key=secret_key, base_url=base_url, request_session=request_session)

    # --------- UM (U本位合约) ---------
    def um_create_order(self, symbol, side, type_, quantity, price=None, positionSide=None, reduceOnly=None, timeInForce=None, clientOrderId=None, **kwargs):
        """
        统一账户 U本位合约下单
        """
        uri = "/papi/v1/um/order"
        params = {
            "symbol": symbol,
            "side": side,
            "type": type_,
            "quantity": quantity
        }
        if price is not None:
            params["price"] = price
        if positionSide is not None:
            params["positionSide"] = positionSide
        if reduceOnly is not None:
            params["reduceOnly"] = reduceOnly
        if timeInForce is not None:
            params["timeInForce"] = timeInForce
        if clientOrderId is not None:
            params["newClientOrderId"] = clientOrderId
        params.update(kwargs)
        return self.request("POST", uri, params, auth=True)

    def um_cancel_order(self, symbol, orderId=None, origClientOrderId=None):
        """
        统一账户 U本位合约撤单
        """
        uri = "/papi/v1/um/order"
        params = {"symbol": symbol}
        if orderId is not None:
            params["orderId"] = orderId
        if origClientOrderId is not None:
            params["origClientOrderId"] = origClientOrderId
        return self.request("DELETE", uri, params, auth=True)

    def um_set_position_mode(self, dualSidePosition: bool):
        """
        U本位合约设置持仓模式
        :param dualSidePosition:
        :return:
        """
        uri = "/papi/v1/um/positionSide/dual"
        params = {
            "dualSidePosition": str(dualSidePosition).lower()
        }
        return self.request("POST", uri, params=params, auth=True)

    def um_set_leverage(self, symbol, leverage):
        """
        U本位合约设置杠杆
        :param symbol:
        :param leverage:
        :return:
        """
        uri = "/papi/v1/um/leverage"
        params = {"symbol": symbol, "leverage": leverage}
        return self.request("POST", uri, params=params, auth=True)

    def cm_set_leverage(self, symbol, leverage):
        """
        币本位合约设置杠杆
        :param symbol:
        :param leverage:
        :return:
        """
        uri = "/papi/v1/cm/leverage"
        params = {"symbol": symbol, "leverage": leverage}
        return self.request("POST", uri, params=params, auth=True)

    def get_um_position(self, symbol=None):
        """
        U本位合约持仓信息
        :param symbol:
        :return:
        """
        uri = "/papi/v1/um/positionRisk"
        params = {}
        if symbol:
            params['symbol'] = symbol
        return self.request("GET", uri, params, auth=True)

    def get_cm_position(self, symbol=None):
        """
        币本位合约持仓信息
        :param symbol:
        :return:
        """
        uri = "/papi/v1/cm/positionRisk"
        params = {}
        if symbol:
            params['symbol'] = symbol
        return self.request("GET", uri, params, auth=True)

    def get_um_account_detail(self):
        """
        获取U本位合约账户详情
        """
        uri = "/papi/v1/um/account"
        return self.request("GET", uri, auth=True)

    def get_cm_account_detail(self):
        """
        获取币本位合约账户详情
        """
        uri = "/papi/v1/cm/account"
        return self.request("GET", uri, auth=True)

    def get_um_force_orders(self, symbol=None, auto_close_type=None, start_time: datetime = None, end_time: datetime = None, limit: int = None):
        """
        统一账户查询U本位合约强平记录，未指定auto_close_type时返回强平和ADL订单。
        """
        uri = "/papi/v1/um/forceOrders"
        params = {}
        if symbol:
            params['symbol'] = symbol
        if auto_close_type:
            params['autoCloseType'] = auto_close_type
        if start_time:
            params['startTime'] = int(start_time.timestamp() * 1000) if isinstance(start_time, datetime) else int(start_time)
        if end_time:
            params['endTime'] = int(end_time.timestamp() * 1000) if isinstance(end_time, datetime) else int(end_time)
        if limit:
            params['limit'] = int(limit)
        return self.request("GET", uri, params, auth=True)

    def get_cm_force_orders(self, symbol=None, auto_close_type=None, start_time: datetime = None, end_time: datetime = None, limit: int = None):
        """
        统一账户查询币本位合约强平记录，未指定auto_close_type时返回强平和ADL订单。
        """
        uri = "/papi/v1/cm/forceOrders"
        params = {}
        if symbol:
            params['symbol'] = symbol
        if auto_close_type:
            params['autoCloseType'] = auto_close_type
        if start_time:
            params['startTime'] = int(start_time.timestamp() * 1000) if isinstance(start_time, datetime) else int(start_time)
        if end_time:
            params['endTime'] = int(end_time.timestamp() * 1000) if isinstance(end_time, datetime) else int(end_time)
        if limit:
            params['limit'] = int(limit)
        return self.request("GET", uri, params, auth=True)

