
class BaseError(Exception):
    def __init__(self, error_msg="unknown", error_code=-1, status_code=-1, payload=None) -> None:
        self.error_code = error_code
        self.status_code = status_code
        self.msg = error_msg
        self.payload = payload
        super().__init__(error_msg)

    def __str__(self) -> str:
        return "{}. status code:{}. error code:{}".format(self.msg, self.status_code, self.error_code)

# 交易所错误
class ExchangeError(BaseError):
    pass


# 正在维护
class ExchangeInMaintain(ExchangeError):
    pass

# 认证错误
class AuthenticationError(ExchangeError):
    pass


#权限错误
class PermissionDenied(AuthenticationError):
    pass


#请求即将超限警告
class OutOfRateLimitWarning(ExchangeError):
    pass


#请求次数超限错误
class OutOfRateLimitError(ExchangeError):
    pass


#缺少参数
class ArgumentsRequired(ExchangeError):
    pass

class ArgumentsError(ExchangeError):
    pass

# 服务处理超时
class ServiceTimeout(ExchangeError):
    pass

#网络错误
class NetworkError(BaseError):
    pass

#连接错误
class ConnectionError(NetworkError):
    pass

# 请求超时
class TimeoutError(NetworkError):
    pass

#相应内容错误
class ResponseError(BaseError):
    pass


#使用json解释相应结果出错
class ParseJsonError(ResponseError):
    pass