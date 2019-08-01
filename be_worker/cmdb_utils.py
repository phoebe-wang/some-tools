#!/usr/bin/env python
# encoding:utf-8

import requests
import traceback

STATUS_MAP = {
               0 : "ok",
               1 : "disconnected",
               2 : "timeout"
             }

def query_url(url, retry=3, timeout=2):
    n = retry
    # 0 is ok, 1 is disconnected, 2 is timeout
    status = 0
    data = ""
    while n > 0:
        try:
            r = requests.get(url, timeout=timeout)
            r.encoding = 'utf-8'
            if r.status_code / 100 == 2:
                data = r.text
                status = 0
                break
            else:
                status = 1
                data = "[Error]: Can not get url: %s:%s try: %s " % (url, r.status_code, retry-n+1)
        except requests.exceptions.ConnectionError:
            # disconnected
            status = 1
            data = "Connect failed: %s: try: %s :%s" % (url, retry-n+1, traceback.format_exc())
        except requests.exceptions.ReadTimeout:
            # timeout
            status = 2
            data = "Timeout: %s: try: %s :%s" % (url, retry-n+1, traceback.format_exc())
        finally:
            n -= 1
    return status, data


def send_request(url, method, data):
    try:
        headers= {'Content-Type':'application/json'}
        res = eval("requests.%s(url, data=data, headers=headers)" % method)
        if res.status_code not in (200, 202):
            return False, "[Error]: Can not %s data: %s:%s" % (method, url, res.status_code)
    except BaseException:
        return False, "%s url failed: %s:%s" % (method, url, traceback.format_exc())
    return True, res


def get_request(url, headers, timeout=1, retry=3):
    resp = None
    while retry > 0:
        try:
            resp = requests.get(url=url, headers=headers, timeout=timeout)
            if resp.status_code / 100 == 2:
                break
            else:
                retry -= 1
                continue
        except:
            retry -= 1
            continue
    return resp
