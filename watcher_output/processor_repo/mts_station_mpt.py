# -*- coding: utf-8 -*-
import re
import socket
import time

import requests
import requests.exceptions
from influxdb import InfluxDBClient
from logbook import Logger

"""
Stmgr Logstreamer

"""

__author__ = "majj"
__copyright__ = "majj"
__license__ = "MIT"
__version__ = "0.0.3"
log = Logger("http.st")


class StmgrDecoder(object):
    """ parser
    """

    def __init__(self, conf):
        self.conf = conf
        rawstr = r'^\((.*)\) (.*) \[(.*)\] "(.*)"'
        self.compile_obj = re.compile(rawstr)
        self.last_timestamp = 0
        self.p = 0
        self.logstr = ""
        self.status = None
        self.status_map = {'running': 1, 'error': 2, 'stop': 0, 'unknown': 3}

    def build_fields(self, line):
        match_obj = self.compile_obj.search(line)
        if match_obj is not None:
            all_groups = match_obj.groups()
            log_time_str, level_msg, logger_name, log_msg = all_groups
            logger_name = logger_name.split(" ")[0]
            level = self.conf['level'].get(level_msg, 8)
            logtime = time.strptime(log_time_str, '%m/%d/%Y %H:%M:%S')
            # timestr = time.strftime("%Y-%m-%d %H:%M:%S", logtime)
            timestamp = time.mktime(logtime)
            # self.status = self.conf['define']['status'].get(log_msg, None)
            self.status_set = self.conf['status']

            str_status = StmgrDecoder.calculate_status(log_msg, self.status_set)
            if str_status is not None:
                self.status = self.status_map[str(str_status)]
                if level == 3:
                    self.status = 2
                    # print self.status, log_msg
                    # print self.status
            else:
                self.status = None
            if self.last_timestamp == 0:
                self.last_timestamp = timestamp
                duration = 0
            else:
                duration = timestamp - self.last_timestamp
                self.last_timestamp = timestamp
            # build message
            data = {
                "duration": duration, "time": timestamp,
                "logger": logger_name, "level": level, "msg": log_msg,
                "status": self.status}
            if self.status is None:
                log.debug("don't has status,we del it")
                del data["status"]
            return data
        else:
            return None

    @staticmethod
    def calculate_status(log_msg, status_set):
        for one_set in status_set:
            if log_msg.startswith(tuple(status_set[one_set])):
                status = one_set
                return status
        return None


class InfluxDBBase(object):
    """ influxdb lib """

    def __init__(self, config):
        """ init """
        log.debug("init InfluxDBBase...")
        self.config = config["influxdb"]
        self.client = InfluxDBClient(self.config['host'],
                                     self.config['port'], self.config['username'],
                                     self.config['password'], self.config['db'])

    def send(self, json_body):
        """ write point """
        self.client.write_points(json_body, time_precision='u')


class Outputer(InfluxDBBase):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.seq = 0
        self.HEADERS = {'content-type': 'application/json', 'accept': 'json', 'User-Agent': 'mabo'}
        self.hostname = socket.gethostname()
        self._session = requests.Session()

        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.parser = StmgrDecoder(conf['processor'][processor])
        super(Outputer, self).__init__(conf)

    def message_process(self, msgline, task, measurement, inject_tags):
        error_dict = dict()
        if msgline.startswith('***') or msgline.isalpha():
            return 0, None
        try:
            data = self.parser.build_fields(msgline)
        except Exception as e:
            print(e)
            # error_dict['info'] = ('http_st_102', msgline)
            # return 1, error_dict
        self.seq += 1

        if data is None:
            # parse msgline failed

            tags = {"hostname": self.hostname, "node": self.nodename, "task": task, "seq": self.seq}
            if inject_tags is not None:
                tags.update(inject_tags)
            fields = {"line": msgline, "datetime": time.strftime("%Y-%m-%d %H:%M:%S")}
            # measurement: issueline
            payload = {"data": {
                "tags": tags, "fields": fields,
                "time": int(time.time()), "measurement": "issueline"}}
        else:
            # parse msgline success
            if "status" in data:
                fields = {
                    "status": data["status"],
                    "Msg": data["msg"],
                    "logger": data["logger"],
                    "hostname": self.hostname,
                    "task": task,
                    "seq": self.seq,
                    "FLevel": data["level"]}
            else:
                fields = {
                    "Msg": data["msg"],
                    "logger": data["logger"],
                    "hostname": self.hostname,
                    "task": task,
                    "seq": self.seq,
                    "FLevel": data["level"]

                }
            tags = {
                "Level": data["level"],
                "eqpt_no": self.eqpt_no,
            }
            if inject_tags is not None:
                tags.update(inject_tags)
            payload = {"data": {"tags": tags,
                                "fields": fields,
                                "time": 1000000 * int(data["time"]) + self.seq % 1000,
                                "measurement": measurement}}

        log.debug("this is data we send to influxdb ")
        self.send([payload["data"]])
        # with open('data.txt', 'wt') as f:
        #     f.write(str(payload['data']))
        return 0, None


def test():
    import toml
    with open("watchman.toml") as conf_file:
        config = toml.loads(conf_file.read())
    out_tester = Outputer(config, 'MTSLogOutput')
    pth = "C:\\MTS 793\\Controllers\\PATAC_FT\\Config\\"
    logs = ['Spindle_Direct_38401000.log', 'Spindle_Matrix_38401.log', 'Spindle_Matrix_with LR000.log',
            'Spindle_Matrix_with LR001.log']
    i = 0
    for logfile in logs:
        # print "this is log {0}".format(i + 1)
        # print"log name :{0}".format(logfile)
        # print "**" * 30
        if i == 4:
            break
        fh = open(pth + logfile, 'r')
        i = i + 1
        for msgline in fh:
            out_tester.message_process(msgline, 'task', 'MTS_Station', 'full')


if __name__ == "__main__":
    test()
