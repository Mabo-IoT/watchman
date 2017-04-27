# -*- coding: utf-8 -*-
import time

import pendulum
from logbook import Logger

log = Logger('eps_rpc')


class Outputer(object):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.conf = conf
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.seq = 1

    def message_process(self, msgline, task, measurement):
        try:
            date_temp, str_temp, log_msg = msgline.split(' ', 2)
            time_str = date_temp + str_temp + log_msg[:2]
            timestamp = pendulum.from_format(time_str, '%m/%d/%Y%H:%M:%S%p', 'Asia/Shanghai').int_timestamp
            self.seq += 1
            temp = log_msg[3:].split('\t')
            unknown_1, unknow_2, unknow_3 = temp[0], temp[1], temp[-1]

            # construct json
            fields = {'parameter': unknown_1, 'logger': unknow_2, 'Msg': unknow_3}
            tags = {'eqpt_no': self.eqpt_no, 'nodename': self.nodename, 'task': task}

            influx_json = {"tags": tags,
                           "fields": fields,
                           "time": 1000000 * int(timestamp) + self.seq % 1000,
                           "measurement": measurement}

            return 0, 'process successful', influx_json
        except ArithmeticError as e:
            influx_json = {
                "fields": {'msg': msgline},
                "time": 1000000 * int(time.time()),
                "measurement": 'new_issueline'}
            return 1, 'wrong format.', influx_json
