
import requests

__GLOBAL_SESSIONS = {}
__PROXY = None  # type: [None, str]


def get_session(base_url=None):
    if base_url not in __GLOBAL_SESSIONS:
        sess = requests.Session()
        if __PROXY:
            sess.proxies['http'] = __PROXY
            sess.proxies['https'] = __PROXY
        __GLOBAL_SESSIONS[base_url] = sess
    return __GLOBAL_SESSIONS[base_url]


def make_session():
    s = requests.Session()
    if __PROXY:
        s.proxies['http'] = __PROXY
        s.proxies['https'] = __PROXY
    return s


def set_proxy(proxy):
    global __PROXY
    __PROXY = proxy
    for sess in __GLOBAL_SESSIONS.values():
        if __PROXY:
            sess.proxies['http'] = __PROXY
            sess.proxies['https'] = __PROXY
        else:
            del sess.proxies['http']
            del sess.proxies['https']
