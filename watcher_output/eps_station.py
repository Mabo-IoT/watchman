# -*- coding: utf-8 -*-
from logbook import Logger
import socket
import pendulum
import requests
import time
import requests.exceptions
from influxdb import InfluxDBClient

"""
Stmgr Logstreamer


"""

__author__ = "Marshall Fate"
__copyright__ = "Marshall Fate"
__license__ = "MIT"
__version__ = "0.0.0"
log = Logger("eps_station")


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


class StmgrDecoder(object):
    """ parser
    """

    def __init__(self, conf, processor):
        self.conf = conf['processor'][processor]
        self.last_timestamp = 0
        self.p = 0
        self.logstr = ""
        self.status = None
        self.status_map = {'running': 1, 'error': 2, 'stop': 0, 'unknown': 3}

    def build_fields(self, line):
        if not line.startswith('('):
            return None
        time_temp1, time_temp2, level_msg, logger_name, log_msg = line.split(' ', 4)
        level = self.conf['level'].get(level_msg, 8)
        time_str = (time_temp1 + time_temp2)[1:-1]
        timestamp = pendulum.from_format(time_str, '%m/%d/%Y%H:%M:%S', 'Asia/Shanghai').timestamp
        status_set = self.conf['status']
        str_status = StmgrDecoder.calculate_status(log_msg, status_set)
        if str_status is not None:
            self.status = self.status_map[str(str_status)]
            if level == 3:
                self.status = 2
        else:
            self.status = None

        # pack field message
        data = {
            "time": timestamp, "logger": logger_name[1:-1],
            "level": level, "msg": log_msg,
            "status": self.status}
        if self.status is None:
            del data["status"]
        return data

    @staticmethod
    def calculate_status(log_msg, status_set):
        for one_set in status_set:
            if log_msg.startswith(tuple(status_set[one_set])):
                status = one_set
                return status
        return None


class Outputer(InfluxDBBase):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.seq = 0
        self.HEADERS = {'content-type': 'application/json', 'accept': 'json', 'User-Agent': 'mabo'}
        self.hostname = socket.gethostname()
        self._session = requests.Session()

        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.parser = StmgrDecoder(conf, processor)
        super(Outputer, self).__init__(conf)

    def message_process(self, msgline, task, measurement, inject_tags):
        error_dict = dict()
        try:
            data = self.parser.build_fields(msgline)
        except:
            error_dict['info'] = ('eps_station', msgline)
            return 1, error_dict
        if data is None:
            return 0, None
        self.seq += 1

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

        self.send([payload['data']])
        log.debug('eps_station sent!')
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
