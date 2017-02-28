# -*- coding: utf-8 -*-
import re
import time
import traceback

import pendulum
from logbook import Logger

log = Logger("mts_station_mpt")


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
        self.status_set = self.conf['status']

    def build_fields(self, line):
        match_obj = self.compile_obj.search(line)
        if match_obj is not None:
            log_time_str, level_msg, logger_name, log_msg = all_groups = match_obj.groups()

            logger_name = logger_name.split(" ")[0]

            level = self.conf['level'].get(level_msg, 8)

            logtime = time.strptime(log_time_str, '%m/%d/%Y %H:%M:%S')

            timestamp = time.mktime(logtime)

            str_status = StmgrDecoder.calculate_status(log_msg, self.status_set)
            if str_status is not None:
                self.status = self.status_map[str(str_status)]
                if level == 3:
                    self.status = 2
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
                log.info("don't has status, del it")
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


class Outputer(object):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.seq = 0
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.parser = StmgrDecoder(conf['processor'][processor])

    def message_process(self, msgline, task, measurement):

        data = self.check_valid(msgline)
        self.seq += 1
        if data == 0 or data == 1:
            return data

        if data is None:
            # parse msgline failed

            tags = {"node": self.nodename, "task": task, "seq": self.seq}

            fields = {"line": msgline, "datetime": time.strftime("%Y-%m-%d %H:%M:%S")}
            # measurement: issueline
            influx_json = {
                "tags": tags,
                "fields": fields,
                "time": pendulum.now().timestamp * 1000000,
                "measurement": "issueline"}
        else:
            # parse msgline success
            if "status" in data:
                fields = {
                    "status": data["status"],
                    "Msg": data["msg"],
                    "logger": data["logger"],

                    "task": task,
                    "seq": self.seq,
                    "FLevel": data["level"]}
            else:
                fields = {
                    "Msg": data["msg"],
                    "logger": data["logger"],

                    "task": task,
                    "seq": self.seq,
                    "FLevel": data["level"]

                }
            tags = {
                "Level": data["level"],
                "eqpt_no": self.eqpt_no,
            }

            influx_json = {"tags": tags,
                           "fields": fields,
                           "time": 1000000 * int(data["time"]) + self.seq % 1000,
                           "measurement": measurement}

        return influx_json

    def check_valid(self, msgline):
        if msgline.startswith('***') or msgline.isalpha():
            log.info('unexpected msg_line, pass.')
            return 0
        try:
            data = self.parser.build_fields(msgline)
        except Exception as e:
            log.info('can\'t parse this msg_line.')
            log.error(e)
            log.error(traceback.format_exc())
            return 1
        return data
