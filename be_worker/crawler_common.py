#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time
import logging
import simplejson as json
import requests
import traceback
from logging.handlers import RotatingFileHandler
import math
from cmdb_utils import query_url, send_request, STATUS_MAP, get_request


def get_json_jdos_config(fileContent):
    info = {}
    lines = fileContent.split("\n")
    for line in lines:
        if line.find("export ") != -1:
            start = line.find("export ") + 7
            end_key = line.find("=")
            key = line[start: end_key]
            value = line[end_key+1: ]
            info[key] = value
        else:
            pass
    return info


def get_logger(level="INFO", logpath="/export/logs/cmdb/be_worker/", logfile=None):
    logger = logging.getLogger()
    eval("logger.setLevel(logging.%s)" % level)
    fh = RotatingFileHandler("%s/%s" % (logpath, logfile), mode='a', maxBytes=10485760, backupCount=3, encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    return logger


class CmdbHandler():

    def __init__(self, host, port, system_name, app_name, logger):

        '''初始化'''
        self.host = host
        self.port = port
        self.logger = logger
        self.system_name = system_name
        self.app_name = app_name

    def update_hosts(self, hosts):
        add_groups, update_groups, delete_groups = self._compare_hosts(hosts)
        ret, res = send_request("http://%s:%s/api/v1.0/hosts/bulk/" % (self.host, self.port), 'post', \
                   json.dumps({"add": add_groups, "update": update_groups, "delete": delete_groups}))
        if ret == False:
            self.logger.error("add hosts failed %s" % res)
        return ret

    def get_last_groups(self):
        param = "systemName=%s&appName=%s" % (self.system_name, self.app_name)
        url = "http://%s:%s/api/v1.0/hosts/?&%s" % (self.host, self.port, param)
        ret, res = query_url(url)
        if ret:
            self.logger.error(res)
            return None
        last_groups = json.loads(res)
        return last_groups


    def _compare_hosts(self, hosts):
        add_groups = []
        delete_groups = []
        update_groups = []
        last_groups = self.get_last_groups()
        for last in last_groups:
            group_name = last["groupName"]
            delete_flag = True
            if hosts:
                for new in hosts:
                    if new["groupName"] == group_name:
                        delete_flag = False
                        new["_id"] = last["_id"]
                        update_groups.append(new)
                        hosts.remove(new)
                        break
            if delete_flag:
                delete_groups.append(last)
        add_groups = hosts
        return add_groups, update_groups, delete_groups

    def get_cluster_config(self):
        url = "http://%s:%s/api/v1.0/config/" % (self.host, self.port)
        ret, res = query_url(url)
        if ret:
            self.logger.error(res)
            return None
        config = json.loads(res)
        return config


class JdosHandler():

    def __init__(self, domain, erp, token, logger):
        self.domain = domain
        self.erp = erp
        self.token = token
        self.logger = logger


    def get_groups(self, systemName, appName):
        res = {}
        try:
            url_instance = "http://{jdos_domain}/api/system/{systemName}/app/{appName}/group"\
                           .format(jdos_domain=self.domain,systemName=systemName, appName=appName)
            msg = ''
            headers = {"Content-type": "application/json", "token": self.token, "erp": self.erp}
            result = get_request(url_instance, headers=headers)
            if result:
                resp_info = json.loads(result.text)
                if not resp_info["success"]:
                    self.logger.error("Get all group info failed: %s" % result.text)
                    return None
                else:
                    res = resp_info["data"]
            else:
                self.logger.error("获取分组信息失败")
                return None
        except:
            self.logger.error("Get all group info failed: %s" % traceback.format_exc())
            return None

        return res

    def jdos_get_group_podlist(self, systemName, appName, groups):
        try:
            data = []
            for group in groups:
                url_instance = "http://{jdos_domain}/api/system/{systemName}/app/{appName}/group/{groupName}/cluster/podall"\
                               .format(jdos_domain=self.domain, systemName=systemName, appName=appName, groupName=group)
                msg = ''
                headers = {"Content-type": "application/json", "token": self.token, "erp": self.erp}
                result = get_request(url_instance, headers=headers)
                if result:
                    resp_info = json.loads(result.text)
                    if not resp_info["success"]:
                        self.logger.error("Get all podlist info failed: %s" % result.text)
                        return None
                    else:
                        data.append({group:resp_info["data"]})
                else:
                    self.logger.error("获取机器列表信息失败")
                    return None
        except:
            self.logger.error("Get all podlist info failed: %s" % traceback.format_exc())
            return None

        return data


    def get_group_config(self, systemName, appName):
        groups = self.get_groups(systemName, appName)
        config = {}
        if groups:
            for i in groups:
                groupName = i['groupName']
                nickname = i['nickname']
                config[groupName] = nickname
        return config

    def get_jdos_hosts(self, systemName, appName, jdos_groups):
        # get group nickname
        group_config = self.get_group_config(systemName, appName)
        # get hosts
        groups = self.jdos_get_group_podlist(systemName, appName, jdos_groups)
        hosts = []
        if not groups:
            self.logger.warn('no hosts in groups %s' % jdos_groups)
            return []
        for group in groups:
            for group_name in group:
                nickname = group_config.get(group_name)
                for h in group[group_name]:
                    tmp = {}
                    tmp['ip'] = h['podIP']
                    tmp['podName'] = h['podName']
                    tmp['groupName'] = group_name
                    tmp['nickName'] = nickname
                    hosts.append(tmp)
        return hosts

    def jdos_get_cluster_config(self, systemName, appName, groupName):
        failed_msg = ''
        resp = False
        result = None
        try:
            url_instance = "http://{jdos_domain}/api/system/{systemName}/app/{appName}/group/{groupName}/cluster/info"\
                           .format(jdos_domain=self.domain, systemName=systemName, appName=appName, groupName=groupName)
            headers = {"Content-type": "application/json", "token": self.token, "erp": self.erp}
            res = get_request(url_instance, headers=headers)
            if res:
                resp_info = json.loads(res.text)
                if not resp_info["success"]:
                    logger.error("Get jdos group %s config info failed: %s" % (groupName, res.text))
                    failed_msg = resp_info["message"]
                else:
                    resp = True
                    result = resp_info["data"]
            else:
                failed_msg = "获取jdos clusterinfo请求返回为空！"
        except:
            logger.error("Get jdos groupconfig failed: %s" % traceback.format_exc())
            failed_msg = "internal server error"

        return resp, failed_msg, result

    def jdos_get_config_file(self, systemName, appName, groupName, uuid):
        failed_msg = ''
        resp = False
        result = None
        try:
            url_instance = "http://{jdos_domain}/api/system/{systemName}/app/{appName}/group/{groupName}/config/file/info/{uuid}"\
                           .format(jdos_domain=self.domain, systemName=systemName, appName=appName, groupName=groupName, uuid=uuid)
            headers = {"Content-type": "application/json", "token": self.token, "erp": self.erp}
            res = get_request(url_instance, headers=headers)
            if res:
                resp_info = json.loads(res.text)
                if not resp_info["success"]:
                    logger.error("Get jdos group %s config info failed: %s" % (groupName, res.text))
                    failed_msg = resp_info["message"]
                else:
                    resp = True
                    result = resp_info["data"]
            else:
                failed_msg = "获取jdos configfileinfo请求返回为空！"
        except:
            logger.error("Get jdos groupconfig failed: %s" % traceback.format_exc())
            failed_msg = "internal server error"

        return resp, failed_msg, result
