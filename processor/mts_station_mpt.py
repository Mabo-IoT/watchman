# -*- coding: utf-8 -*-
import os
import re
import time

from logbook import Logger

log = Logger("mts_station_mpt")


class msg_parser(object):
    """
    msgline parser.
    """

    def __init__(self):
        rawstr = r'^\((.*)\) (.*) \[(.*)\] "(.*)"'
        self.compile_obj = re.compile(rawstr)


class Outputer(object):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.seq = 0
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]

        # get a parser.
        self.parser = msg_parser()

        # for status.
        self.status = None
        self.status_map = {'running': 1, 'error': 2, 'stop': 0, 'unknown': 3}
        self.status_set = conf['processor'][processor]['status']

        # for level
        self.level = conf['processor'][processor]['level']

        # for recording
        self.last_timestamp = 0

    def message_process(self, msgline, task_related, measurement):
        """接受一条日志，解析完成后，取得相应信息，组成influxdb
        所需字典，返回。

        :param msgline: 日志
        :param task_related: 相关信息
        :param measurement: 此实验的表名
        :return: 以influxdb line protocal 组成的字典
        """

        not_valid = Outputer.check_valid(msgline)
        if not_valid:
            influx_json = {
                "fields": {'msg': msgline},
                "time": 1000000 * int(time.time()),
                "measurement": 'new_issueline'}
            return 1, 'wrong format.', influx_json

        some_data = self.process_data(msgline)
        if some_data is None:
            influx_json = {
                "fields": {'msg': msgline},
                "time": 1000000 * int(time.time()),
                "measurement": 'new_issueline'}
            return 1, 'wrong format.', influx_json

        data = self.build_fields(some_data)
        filename, self.task_infomation = task_related
        task = self.get_task(filename)
        # construct influx json.
        if "status" in data:
            fields = {
                "status": data["status"],
                "Msg": data["msg"],
                "logger": data["logger"],

                "task": task,

                "FLevel": data["level"]}
        else:
            fields = {
                "Msg": data["msg"],
                "logger": data["logger"],

                "task": task,

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
        # seq value.
        if self.seq > 10000:
            self.seq = 0
        self.seq += 1
        return 0, 'process successful', influx_json

    @staticmethod
    def calculate_status(log_msg, status_set):
        for one_set in status_set:
            if log_msg.startswith(tuple(status_set[one_set])):
                status = one_set
                return status
        return None

    def build_fields(self, *args):
        """get raw message, return food influx_json.
        :param args:
        :return: dict, influx_json
        """
        timestamp, level_msg, logger_name, log_msg = args[0]

        level = self.level.get(level_msg, 8)

        str_status = Outputer.calculate_status(log_msg, self.status_set)

        if str_status is not None:
            self.status = self.status_map[str(str_status)]
            if level == 3:
                self.status = 2
        else:
            self.status = None

        # for recoding how long between this message and the former.
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
            del data["status"]

        return data

    def process_data(self, msgline):
        is_match = self.parser.compile_obj.search(msgline)
        if is_match is not None:
            time_str, level_msg, logger_name, log_msg = all_groups = is_match.groups()

            # make time_stamp
            logtime = time.strptime(time_str, '%m/%d/%Y %H:%M:%S')
            timestamp = time.mktime(logtime)

            # exclude no used info
            log_msg = log_msg.strip('"')

            return timestamp, level_msg, logger_name, log_msg
        else:
            return None

    @staticmethod
    def check_valid(msgline):

        if msgline.startswith('***') or msgline.isalpha():
            log.info('unexpected msg_line, pass.')
            return True
        else:
            return False

    def get_task(self, file_absolute_path, ):

        some = self.task_infomation
        if isinstance(some, int):
            name = file_absolute_path.split(os.sep)[some]
            # if .xxx ,drop it
            if '.log' or '.txt' in name:
                name = name[:-4]
            task = name
        else:
            task_map = dict([tuple(one.split(':')) for one in some])
            path_split = file_absolute_path.split(os.sep)

            for one in task_map:
                if one in path_split:
                    task = task_map[one]
                    return task
            task = 'full'
        return task
# fixme :RPC也要分前悬后悬的分离。未做