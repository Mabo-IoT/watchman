# -*- coding: utf-8 -*-
from sys import version_info

import pendulum
from influxdb import InfluxDBClient
from logbook import Logger

if version_info[0] == 3:
    from re import fullmatch
else:
    from re import match

log = Logger('tts_history')


def one(pre_time_str, log_str):
    str_list = log_str.split()
    code = str_list[0]
    message = ' '.join(str_list[1:-3])
    post_time_str = ' '.join(str_list[-3:])
    fields = {'code': code, 'message': message}
    if version_info[0] == 3:
        time_str = pre_time_str + post_time_str.split(maxsplit=1)[-1]
    else:
        time_str = pre_time_str + post_time_str.split(None, 1)[-1]
    timestamp = pendulum.from_format(time_str, '[%Y/%m/%d]%I:%M:%S %p>').timestamp
    return fields, timestamp


def two(pre_time_str, log_str):
    _ = pre_time_str
    fields = {temp[0]: float(temp[1:].strip()) for temp in log_str.split(',')}
    timestamp = pendulum.now('Asia/Shanghai').timestamp
    return fields, timestamp


def three(pre_time_str, log_str):
    _ = pre_time_str
    timestamp = pendulum.now('Asia/Shanghai').timestamp
    fields = {'message': log_str}
    return fields, timestamp


def four(pre_time_str, log_str):
    _ = pre_time_str
    position_list = log_str[6:].strip().split(',')
    fields = {temp[0]: float(temp[1:].strip()) for temp in position_list}
    fields['circle'] = 1
    timestamp = pendulum.now('Asia/Shanghai').timestamp
    return fields, timestamp


def five(pre_time_str, log_str):
    _ = pre_time_str
    fields = {each[0]: float(each[1:]) for each in log_str.split()[1:]}
    fields['N00000'] = 1
    timestamp = pendulum.now('Asia/Shanghai').timestamp
    return fields, timestamp


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
    feature = {'(\w\d{3}|\w+\d_\d{3})\s+.*>$': one,
               'X.+,Y.+,Z.+': two,
               '\w+.+\.$': three,
               'CIRCLE.+\d+$': four,
               'N\d+.+\d+$': five}

    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.conf = conf
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.status_map = config_conf['status']
        self.seq = 1

        super(Outputer, self).__init__(conf)

    def message_process(self, msgline, task, measurement, inject_tags):
        pre_time_str, log_str = msgline[:12], msgline[12:]

        for each_pattern in Outputer.feature:
            if version_info[0] == 3:
                res = fullmatch(each_pattern, log_str)
            else:
                res = match(each_pattern, log_str)

            if res:
                fields, timestamp = Outputer.feature[each_pattern](pre_time_str, log_str)
                payload = {'fields': fields,
                           'time': 1000000 * timestamp + self.seq % 1000,
                           'measurement': measurement,
                           'tags': {
                               'eqpt_no': self.eqpt_no,
                               'nodename': self.nodename}
                           }
                self.send([payload])
        log.debug('sent!')
        return 0, None

    def get_status(self, code):
        for each in self.status_map:
            if code[0].lower().startswith(each[0]):
                return self.status_map[each]


def pre_process(msgline):
    pre_time_str, log_str = msgline[:12], msgline[12:]

    for each_pattern in Outputer.feature:
        if version_info[0] == 3:
            res = fullmatch(each_pattern, log_str)
        else:
            res = match(each_pattern, log_str)
        if res:
            code, message, post_time_str = Outputer.feature[each_pattern](log_str)
            if post_time_str is None:
                return None, code, message
            if version_info[0] == 3:
                time_str = pre_time_str + post_time_str.split(maxsplit=1)[-1]
            else:
                time_str = pre_time_str + post_time_str.split(None, 1)[-1]
            return time_str, code, message
