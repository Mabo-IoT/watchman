# -*- coding: utf-8 -*-
import re
from collections import OrderedDict

import pendulum
from logbook import Logger

log = Logger('shake window')


class Outputer(object):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.conf = conf
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.status_map = config_conf['status']
        self.seq = 1

    def message_process(self, msgline, task, measurement):
        _ = task
        raw_time_str, status_str, frequency_str = msgline[:19], msgline[26:31], msgline[36:]
        timestamp = pendulum.from_format(raw_time_str, '%Y_%m_%d_%H_%M_%S', 'Asia/Shanghai').timestamp
        status = self.get_status(status_str)
        fields = Outputer.get_field(frequency_str)
        fields['status'] = status
        tags = {'eqpt_no': self.eqpt_no, 'nodename': self.nodename}
        influx_json = OrderedDict({'tags': tags, 'fields': fields,
                                   'time': 1000000 * int(timestamp) + self.seq % 1000,
                                   'measurement': measurement, })

        return influx_json

    def get_status(self, status_str):
        for one in self.status_map:
            if status_str.startswith(one):
                return self.status_map[one]

    @staticmethod
    def get_field(frequency_str):
        pattern = '[ABCD]:\d+\s+,\d+\s+'
        res = re.findall(pattern, frequency_str)
        fields = {}
        for one in res:
            fields[one[0] + '_expect'] = int(one[2:9].strip(' '))
            fields[one[0] + '_now'] = int(one[13:].strip(' '))
        return fields
