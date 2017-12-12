# -*- coding: utf-8 -*-
import os
import re
import time

import pendulum
from logbook import Logger

log = Logger("eps_station")


class Outputer(object):
    def __init__(self, conf, processor):
        processor_conf = conf['processor'][processor]

        # eqpt_info
        self.nodename = processor_conf["config"]["nodename"]
        self.eqpt_no = processor_conf["config"]["eqpt_no"]

        # status info
        self.status_set = processor_conf['status']
        self.status_map = {'running': 1, 'error': 2, 'stop': 0, 'unknown': 3}

        # level info
        self.level = conf['processor'][processor]['level']

        # parser
        self.parser = self.get_parser()

        self.seq = 0

    def message_process(self, msgline, task, measurement):

        if check_valid(msgline):

            # get string info.
            temp_info = self.match(msgline)
            if temp_info == 'not match':
                return 2, 'wrong format', None

            # make time
            time_stamp = self.get_time(temp_info)

            # get_logger
            logger = self.get_logger(temp_info)

            # make fields cosist of logger, msgline
            msg = self.get_msg(temp_info)

            # get task.
            task = self.get_task(task)

            # get level
            level = self.get_level(temp_info)

            influx_json = self.construct_json(time_stamp, msg, logger, measurement, level, task)

            self.make_seq()

            process_rtn = 0
            info = 'good'
            return process_rtn, info, influx_json
        else:
            return 2, 'wrong format', None

    def get_time(self, temp_info):

        time_str = temp_info[0]

        dt = pendulum.from_format(time_str, '%m/%d/%Y %H:%M:%S', 'Asia/Shanghai')

        return int(dt.float_timestamp) * 1000000 + self.seq

    def get_logger(self, temp_info):

        return temp_info[2]

    def get_msg(self, temp_info):

        return temp_info[-1]

    def get_task(self, task_info):
        task = task_info[0].split(os.sep)[task_info[-1]][:-4]
        return task

    def construct_json(self, timestamp, msg, logger, measurement, level, task):
        # caculate status
        status = self.calculate_status(msg)

        # status <-> level
        if level == 3:
            status = 2

        fields = {
            'Msg': msg,
            'logger': logger,
            'status': status,
            'FLevel': level,
            'task': task
        }

        tags = {
            "node": self.nodename,
            "eqpt_no": self.eqpt_no,
            'Level': level
        }

        payload = {"tags": tags,
                   "fields": fields,
                   "time": timestamp,
                   "measurement": measurement}

        return payload

    def calculate_status(self, log_msg, ):
        for one_set in self.status_set:
            if log_msg.startswith(tuple(self.status_set[one_set])):
                status = one_set
                return self.status_map[status]
        return None

    def get_level(self, temp_info):
        return self.level.get(temp_info[1], 6)

    def make_seq(self, ):
        self.seq += 1
        if self.seq > 1000:
            self.seq = 0

    def get_parser(self):
        rawstr = r'^\((.*)\) (.*) \[(.*)\] "(.*)"'
        parser = re.compile(rawstr)
        return parser

    def match(self, msgline):
        res = self.parser.search(msgline)
        if res:
            return res.groups()
        else:
            return 'not match'


def unify_level_and_status(status, level):
    return level


def check_valid(msgline):
    if msgline.startswith('('):
        return True
    else:
        log.info('unexpected msg_line, pass.')
        return False
