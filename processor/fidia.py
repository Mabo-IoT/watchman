# -*- coding: utf-8 -*-
import re

import pendulum
from logbook import Logger

log = Logger('Fidia_post')


class Outputer(object):
    def __init__(self, conf, processor):
        processor_conf = conf['processor'][processor]
        self.seq = 0
        self.nodename = processor_conf["config"]["nodename"]
        self.eqpt_no = processor_conf["config"]["eqpt_no"]
        self.level = processor_conf['level']

        self.status_set = processor_conf['status']
        self.status_map = {'error': 2, 'running': 1, 'stop': 0}

        rawstr = r"""L O G    F I L E    C R E A T E D    O N   << (.*) >>"""
        self.compile_obj = re.compile(rawstr)
        self.current_date = None

    def get_date(self, line):
        # common variables
        match_obj = self.compile_obj.search(line)
        if match_obj:
            group_1 = match_obj.group(1)
            d = pendulum.from_format(group_1, '%d-%b-%Y')
            self.current_date = d.to_date_string()

    def message_process(self, msg_line, task, measurement, ):
        self.seq += 1
        self.get_date(msg_line)
        start_character = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
        str_character = tuple((str(i) for i in start_character))

        if msg_line.startswith(str_character):
            data = msg_line.split(' ', 3)
            _, time, code, message = data

            status = self.calculate_status(message, )
            level = Outputer.calculate_level(code)
            if level == 3:
                status = 2
            data = status, level, message, code

            influx_json = self.construct_json(time, data, measurement, task="zan shi wei ding")

            process_rtn = 0
            info = 'good'
            return process_rtn, info, influx_json
            pass
        else:
            return 2, '不是正文', None

    def construct_json(self, time, data, measurement, task):
        fields = {"Msg": data[2],
                  "status": data[0],
                  "Code": data[-1],
                  "Level": self.level[data[1]],
                  "task": task,
                  }

        tags = {
            "node": self.nodename,
            "eqpt_no": self.eqpt_no, }
        dt_str = "{}T{}".format(self.current_date, time)

        dt = pendulum.from_format(dt_str, '%Y-%m-%dT%H:%M:%S', 'Asia/Shanghai')
        payload = {"tags": tags,
                   "fields": fields,
                   "time": int(dt.float_timestamp) * 1000000 + self.seq % 1000,
                   "measurement": measurement}
        return payload

    def calculate_status(self, log_msg, ):
        for one_set in self.status_set:
            if log_msg.startswith(tuple(self.status_set[one_set])):
                status = one_set
                return self.status_map[status]
        return None

    @staticmethod
    def calculate_level(code):
        level_dict = {'F': 'Fatal', 'R': 'Request',
                      'E': 'Error', 'W': 'Warning',
                      'I': 'Information', 'D': 'Debug'}
        level = level_dict.get(code[0])
        return level
