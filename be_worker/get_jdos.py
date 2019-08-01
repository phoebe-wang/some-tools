#!/usr/bin/env python

import crawler_common
import simplejson as json
from optparse import OptionParser
import traceback
logger = crawler_common.get_logger(logfile="get_jdos.log")
import time
import sys


def update_cluster(c, op_host, op_port, jdos_domain, erp, token):
    res = True
    system_name = c['systemName']
    app_name = c['appName']
    config_files = c['configFile']
    new_hosts = []

    jdos_handler = crawler_common.JdosHandler(jdos_domain, erp, token, logger)
    groups = jdos_handler.get_group_config(system_name, app_name)
    for group in groups:
        dup_hosts = []
        has_ip = True
        config = {}
        resp, failed_msg, cluster_config = jdos_handler.jdos_get_cluster_config(system_name, app_name, group)
        if not resp:
            logger.info(failed_msg)
            continue
        elif not cluster_config:
            logger.info("cluster is null in %s,%s,%s" % (system_name, app_name, group))
            has_ip = False
        else:
            files_uuid = cluster_config[0]['groupConfig'].get('configFiles')
            if files_uuid:
                for uuid in files_uuid:
                    resp, failed_msg, c_detail = jdos_handler.jdos_get_config_file(system_name, app_name, group, uuid)
                    if not resp:
                        logger.info(failed_msg)
                        continue
                    file_path = c_detail["filePath"]
                    if file_path in config_files:
                        config = crawler_common.get_json_jdos_config(c_detail["fileContent"])
        if not config:
            logger.info("no config file for group: %s" % group)
        config['systemName'] = system_name
        config['appName'] = app_name
        config['nickName'] = groups[group]
        config['groupName'] = group
        config['hosts'] = []

        if has_ip:
            jdos_hosts = jdos_handler.jdos_get_group_podlist(system_name, app_name, [group])
            jdos_dup_hosts = []
            if not jdos_hosts[0][group]:
                logger.info('no hosts in group: %s' % group)
            else:
                for h in jdos_hosts[0][group]:
                    if h['podIP'] in jdos_dup_hosts:
                        continue
                    jdos_dup_hosts.append(h['podIP'])
                    tmp = {}
                    tmp['ip'] = h['podIP']
                    tmp['podName'] = h['podName']
                    config['hosts'].append(tmp)
            new_hosts.append(config)

    if new_hosts:
        cm_handler = crawler_common.CmdbHandler(op_host, op_port, system_name, app_name, logger)
        res = cm_handler.update_hosts(new_hosts)
    return res

def _main():

    parser = OptionParser()
    parser.add_option('-H', '--host',
                      help='opcenter host.')
    parser.add_option('-p', '--port',
                      help='opcenter port.')
    parser.add_option("-v", action="store_true", dest="verbose")
    parser.add_option('-D', '--domain',
                      help='jdos domain.')
    parser.add_option('-e', '--erp',
                      help='jdos erp.')
    parser.add_option('-t', '--token',
                      help='jdos token.')
    options, args = parser.parse_args()
    op_host = options.host
    op_port = options.port
    jdos_domain = options.domain
    jdos_erp = options.erp
    jdos_token = options.token
    cm_handler = crawler_common.CmdbHandler(op_host, op_port, "", "", logger)
    config  = cm_handler.get_cluster_config()

    if not config:
        logger.error("can not get cluster config")
        sys.exit()
    try:
        for i in config:
            res = update_cluster(i, op_host, op_port, jdos_domain, jdos_erp, jdos_token)
            if res:
                logger.info("UPDATE: %s %s %s successful" % (i['systemName'], i['appName'], i['configFile']))
            else:
                logger.error("UPDATE: %s %s %s FAILED" % (i['systemName'], i['appName'], i['configFile']))
                logger.error(traceback.format_exc())
    except Exception:
        logger.error(traceback.format_exc())

if __name__ == '__main__':
    _main()
