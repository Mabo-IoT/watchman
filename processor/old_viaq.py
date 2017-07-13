# -*- coding: utf-8 -*-
import os

import pendulum
from logbook import Logger

log = Logger("mts_station_mpt")


class Outputer:
    def __init__(self, conf, processor):
        processor_conf = conf['processor'][processor]

        self.seq = 0

        # time date
        self.current_date = None
        pass

    def message_process(self, msgline, task, measurement):
        """接受一条日志，解析完成后，取得相应信息，组成influxdb
        所需字典，返回。

        :param msgline: 日志
        :param task: 本次实验任务名
        :param measurement: 此实验的表名
        :return: 以influxdb line protocal 组成的字典
        """

        self.current_date = task[0].split(os.sep)[-1][:-4]

        if check_valid(msgline):
            # make time
            time_stamp = self.get_time(msgline)

            # get_logger
            logger = self.get_logger(msgline)

            # make fields cosist of logger, msgline
            msg = self.get_msg(msgline)

            influx_json = self.construct_json(time_stamp, msg, logger, measurement, )

            self.make_seq()

            process_rtn = 0
            info = 'good'
            return process_rtn, info, influx_json
        else:
            return 2, 'wrong format', None

    def get_time(self, msgline, ):
        temp = msgline.split(' ')[0]
        all_time_str = self.current_date + '-' + temp
        dt = pendulum.from_format(all_time_str, '%Y%m%d-%H:%M:%S', 'Asia/Shanghai')

        return int(dt.float_timestamp) * 1000000 + self.seq

    def get_logger(self, msgline):

        logger = msgline.split(' ')[1]
        return logger

    def get_msg(self, msgline):

        msgline = msgline.split(':')[3]
        return msgline

    def construct_json(self, time, msg, logger, measurement):

        fields = {
            'Msg': msg,
            'logger': logger
        }

        tags = {}

        payload = {"tags": tags,
                   "fields": fields,
                   "time": time,
                   "measurement": measurement}

        return payload

    def make_seq(self, ):
        self.seq += 1
        if self.seq > 1000:
            self.seq = 0


def check_valid(msgline):
    num = len(msgline.split(' ', 2))
    if num == 3:
        return True
    else:
        return False
