# -*- coding: utf-8 -*-
import time
from sys import version_info

import pendulum
from logbook import Logger

if version_info[0] == 3:
    from re import fullmatch
else:
    from re import match

log = Logger('tts_history')


class Outputer(object):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.processor = processor
        self.conf = conf
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.status_map = conf['processor'][processor]['status']
        self.seq = 1

        # for level
        self.level = conf['processor'][processor]['level']

        # for status.
        self.status_map = {'running': 1, 'error': 2, 'stop': 0, 'unknown': 3}
        self.status_set = conf['processor'][processor]['status']

        # for diffrent log format.
        self.feature = {'(\w\d{3}|\w+\d_\d{3})\s+.*>$': self.one,
                        'X.+,Y.+,Z.+': self.two,
                        '\w+.+\.$': self.three,
                        'CIRCLE.+\d+$': self.four,
                        'N\d+.+\d+$': self.five}

    def message_process(self, msgline, task, measurement, ):
        pre_time_str, log_str = msgline[:12], msgline[12:]

        for each_pattern in self.feature:

            # add version compatibilty
            if version_info[0] == 3:
                res = fullmatch(each_pattern, log_str)
            else:
                res = match(each_pattern, log_str)

            if res:
                # if use the one function to process, may return tag and level.
                if self.feature[each_pattern] == self.one:
                    fields, timestamp, level = self.feature[each_pattern](pre_time_str, log_str)
                    influx_json = {'fields': fields,
                                   'time': 1000000 * timestamp + self.seq % 1000,
                                   'measurement': measurement,
                                   'tags': {
                                       'eqpt_no': self.eqpt_no,
                                       'nodename': self.nodename,
                                       'level': level}
                                   }
                    self.seq += 1
                else:

                    fields, timestamp = self.feature[each_pattern](pre_time_str, log_str)

                    influx_json = {'fields': fields,
                                   'time': 1000000 * timestamp + self.seq % 1000,
                                   'measurement': measurement,
                                   'tags': {
                                       'eqpt_no': self.eqpt_no,
                                       'nodename': self.nodename}
                                   }
                    self.seq += 1

                return 0, 'process successful', influx_json

        # wrong format
        influx_json = {
            "fields": {'msg': msgline},
            "time": 1000000 * int(time.time()),
            "measurement": 'new_issueline',
            'tags': {'processor': self.processor, 'eqpt_no': self.eqpt_no}}
        return 1, 'wrong format.', influx_json

    def one(self, pre_time_str, log_str):
        # log_str fragmentization
        str_list = log_str.split()
        message = ' '.join(str_list[1:-3])
        post_time_str = ' '.join(str_list[-3:])
        code = str_list[0]

        # calculate status
        str_status = self.calculate_status(message, self.status_set)
        if str_status is not None:
            status = self.status_map[str(str_status)]
        else:
            status = None

        # get level

        code_first_letter = code[0]
        level_completion = {'I': 'Information', 'W': 'Warning', 'E': 'Error'}
        level = self.level.get(level_completion.get(code_first_letter, None), None)

        # cause service logic, when level is Warning or Error, status must be Error state(2).
        if level == 4 or level == 3:
            status = 2

        fields = {'code': code, 'message': message,
                  'status': status, 'Flevel': level}

        if version_info[0] == 3:
            time_str = pre_time_str + post_time_str.split(maxsplit=1)[-1]
        else:
            time_str = pre_time_str + post_time_str.split(None, 1)[-1]

        timestamp = pendulum.from_format(time_str, '[%Y/%m/%d]%I:%M:%S %p>').int_timestamp
        return fields, timestamp, level

    def two(self, pre_time_str, log_str):
        _ = pre_time_str
        fields = {temp[0]: float(temp[1:].strip()) for temp in log_str.split(',')}
        timestamp = pendulum.now('Asia/Shanghai').int_timestamp
        return fields, timestamp

    def three(self, pre_time_str, log_str):
        _ = pre_time_str
        timestamp = pendulum.now('Asia/Shanghai').int_timestamp
        fields = {'message': log_str}
        return fields, timestamp

    def four(self, pre_time_str, log_str):
        _ = pre_time_str
        position_list = log_str[6:].strip().split(',')
        fields = {temp[0]: float(temp[1:].strip()) for temp in position_list}
        fields['circle'] = 1
        timestamp = pendulum.now('Asia/Shanghai').int_timestamp
        return fields, timestamp

    def five(self, pre_time_str, log_str):
        _ = pre_time_str
        fields = {each[0]: float(each[1:]) for each in log_str.split()[1:]}
        fields['N00000'] = 1
        timestamp = pendulum.now('Asia/Shanghai').int_timestamp
        return fields, timestamp

    @staticmethod
    def calculate_status(log_msg, status_set):

        for one_set in status_set:
            if log_msg.startswith(tuple(status_set[one_set])):
                status = one_set
                return status
        return None

    def pre_process(self, msgline):
        pre_time_str, log_str = msgline[:12], msgline[12:]

        for each_pattern in self.feature:
            if version_info[0] == 3:
                res = fullmatch(each_pattern, log_str)
            else:
                res = match(each_pattern, log_str)
            if res:
                code, message, post_time_str = self.feature[each_pattern](log_str)
                if post_time_str is None:
                    return None, code, message
                if version_info[0] == 3:
                    time_str = pre_time_str + post_time_str.split(maxsplit=1)[-1]
                else:
                    time_str = pre_time_str + post_time_str.split(None, 1)[-1]
                return time_str, code, message
