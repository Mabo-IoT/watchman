# -*- coding: utf-8 -*-

import pendulum
import re
from logbook import Logger

log = Logger('shake window')


class Outputer:
    def __init__(self, conf, processor):
        processor_conf = conf['processor'][processor]
        self.conf = conf
        # eqpt_info
        self.nodename = processor_conf["config"]["nodename"]
        self.eqpt_no = processor_conf["config"]["eqpt_no"]

        # self.status_map = {'error': 2, 'running': 1, 'stop': 0}
        self.status_map = processor_conf['config']['status']
        self.seq = 1

    def message_process(self, msgline, task, measurement):
        """接受一条日志，解析完成后，取得相应信息，组成influxdb
        所需字典，返回。

        :param msgline: 日志
        :param task: 本次实验任务名
        :param measurement: 此实验的表名
        :return: 以influxdb line protocal 组成的字典
        """
        _ = task
        # get time
        timestamp = self.get_time(msgline)

        # get status
        status = self.get_status(msgline)

        fields = self.get_field(msgline)

        influx_json = self.construct_json(timestamp, status, fields, measurement)

        self.make_seq()

        process_rtn = 0
        info = 'good'
        return process_rtn, info, influx_json

    def get_status(self, msg_line):
        status_str = msg_line[26:31]
        for one in self.status_map:
            if status_str.startswith(one):
                return self.status_map[one]

    def get_time(self, msg_line):

        time_str = msg_line[:19]

        dt = pendulum.from_format(time_str, '%Y_%m_%d_%H_%M_%S', 'Asia/Shanghai')

        return int(dt.float_timestamp) * 1000000 + self.seq

    def make_seq(self, ):
        self.seq += 1
        if self.seq > 1000:
            self.seq = 0

    def construct_json(self, timestamp, status, fields, measurement, ):

        fields['status'] = status

        tags = {
            "node": self.nodename,
            "eqpt_no": self.eqpt_no,
        }

        payload = {"tags": tags,
                   "fields": fields,
                   "time": timestamp,
                   "measurement": measurement}

        return payload

    def get_field(self, msg_line):
        frequency_str = msg_line[36:]
        pattern = '[ABCD]:\d+\s+,\d+\s+'
        res = re.findall(pattern, frequency_str)
        fields = {}
        for one in res:
            fields[one[0] + '_expect'] = int(one[2:9].strip(' '))
            fields[one[0] + '_now'] = int(one[13:].strip(' '))
        return fields

# class Outputer(InfluxDBBase):
#     def __init__(self, conf, processor):
#         config_conf = conf['processor'][processor]['config']
#         self.conf = conf
#         self.nodename = config_conf["nodename"]
#         self.eqpt_no = config_conf["eqpt_no"]
#         self.status_map = config_conf['status']
#         self.seq = 1

#         super(Outputer, self).__init__(conf)

#     def message_process(self, msgline, task, measurement, ):
#         _ = task
#         # print(1)
#         raw_time_str, status_str, frequency_str = msgline[:19], msgline[26:31], msgline[36:]
#         timestamp = pendulum.from_format(raw_time_str, '%Y_%m_%d_%H_%M_%S', 'Asia/Shanghai').timestamp
#         status = self.get_status(status_str)
#         fields = Outputer.get_field(frequency_str)
#         fields['status'] = status
#         tags = {'eqpt_no': self.eqpt_no, 'nodename': self.nodename}
#         post_data = {"tags": tags,
#                      "fields": fields,
#                      "time": 1000000 * int(timestamp) + self.seq % 1000,
#                      "measurement": measurement}

#         self.send([post_data])
#         log.debug('good, data is sent!')
#         return 0

#     def get_status(self, status_str):
#         for one in self.status_map:
#             if status_str.startswith(one):
#                 return self.status_map[one]

#     @staticmethod
#     def get_field(frequency_str):
#         pattern = '[ABCD]:\d+\s+,\d+\s+'
#         res = re.findall(pattern, frequency_str)
#         fields = {}
#         for one in res:
#             fields[one[0] + '_expect'] = int(one[2:9].strip(' '))
#             fields[one[0] + '_now'] = int(one[13:].strip(' '))
#         return fields
